import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Callable,
    Iterable,
    Optional,
    Self,
    Type,
)

from aiokafka import (  # pyright: ignore[reportMissingTypeStubs]
    AIOKafkaConsumer,
    AIOKafkaProducer,
    ConsumerRecord,
    TopicPartition,
)
from aiokafka.abc import (  # pyright: ignore[reportMissingTypeStubs]
    ConsumerRebalanceListener,
)
from aiokafka.consumer.subscription_state import (  # pyright: ignore[reportMissingTypeStubs]
    SubscriptionType,
)
from loguru import logger

from ..types import AutoCommitConfig, ParserSpec


@dataclass
class KafkaBaseClient(ABC):
    """
    Group-managed subscription mode:
    - Topics to listen to are determined from ParserSpec at initialization.
    - We subscribe the consumer to those topics and let the group coordinator
      handle partition assignments and rebalancing.
    """

    # ---------- Behavior ----------
    lazy_consumer_start: bool = True
    lazy_producer_start: bool = True
    seek_to_end_on_assign: bool = True  # 새 메시지부터
    metadata_refresh_min_interval_s: float = 5.0
    auto_commit: Optional[AutoCommitConfig] = None
    backoff_min: float = 0.5
    backoff_max: float = 10.0
    backoff_factor: float = 2.0
    assignment_timeout_s: float = 5.0
    rebalance_listener: Optional[ConsumerRebalanceListener] = None

    # ---------- Parser / Correlation ----------
    parsers: Iterable[ParserSpec[object]] = ()
    corr_header_keys: tuple[str, ...] = (
        "request_id",
        "correlation_id",
        "x-correlation-id",
    )
    correlation_from_record: Optional[
        Callable[[ConsumerRecord[bytes, bytes], Optional[object]], Optional[bytes]]
    ] = None
    """Correlation key extractor: (record, parsed or None) -> correlation_id (None if not found)"""

    def __post_init__(self) -> None:
        # 내부 상태
        self._producer_factory: Optional[Callable[[], AIOKafkaProducer]] = None
        self._consumer_factory: Optional[Callable[[], AIOKafkaConsumer]] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._consumer_task: Optional[asyncio.Task[None]] = None
        self._auto_expand_task: Optional[asyncio.Task[None]] = None
        self._closed: bool = True

        # Concurrency protection
        self._start_lock = asyncio.Lock()

        # Parser index (topic -> parser list)
        self._parsers_by_topic: dict[str, list[ParserSpec[object]]] = {}
        # Topics to subscribe to (deduplicated)
        self._subscription_topics: set[str] = set()
        self._collect_parsers_and_assignments(self.parsers)

        # Throttle/commit state
        self._last_md_refresh: float = 0.0
        self._since_commit: int = 0
        self._last_commit: float = time.time()

        # Default correlation key extractor: case-insensitive
        self._corr_header_keys_lower = tuple(k.lower() for k in self.corr_header_keys)
        if self.correlation_from_record is None:
            self.correlation_from_record = self._default_corr_from_record

    # Register parsers and collect static assignments (once at initialization)
    def _collect_parsers_and_assignments(
        self, specs: Iterable[ParserSpec[object]]
    ) -> None:
        for ps in specs:
            # Build topic index and subscription topics (ignore explicit partitioning)
            for topic in ps["topics"]:
                self._parsers_by_topic.setdefault(topic, []).append(ps)
                self._subscription_topics.add(topic)

    # ---------- Lifecycle ----------
    async def start(self) -> None:
        if not self._closed:
            return

        if not self.lazy_producer_start:
            await self._ensure_producer_started()
        if not self.lazy_consumer_start:
            await self._ensure_consumer_started()

        self._closed = False
        logger.info(f"{self.__class__.__name__} started")

        # In subscribe mode, Kafka handles partition changes via rebalancing

    async def stop(self) -> None:
        if self._closed:
            return

        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
            self._consumer_task = None

        if self._consumer is not None and self.auto_commit:
            try:
                await self._consumer.commit()  # pyright: ignore[reportUnknownMemberType]
            except Exception:
                logger.exception("Final commit failed")

        if self._consumer is not None:
            try:
                await self._consumer.stop()
            except Exception:
                logger.exception("Error stopping consumer")
            self._consumer = None

        if self._producer is not None:
            try:
                await self._producer.stop()
            except Exception:
                logger.exception("Error stopping producer")
            self._producer = None

        try:
            await self._on_stop_cleanup()
        except Exception:
            logger.exception("_on_stop_cleanup failed")

        self._closed = True
        logger.info(f"{self.__class__.__name__} stopped")

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.stop()

    # ---------- 내부 공통 처리 ----------
    async def _ensure_producer_started(self) -> AIOKafkaProducer:
        async with self._start_lock:
            if self._producer is not None:
                return self._producer
            if self._producer_factory is None:
                raise ValueError("producer_factory is not set")
            self._producer = self._producer_factory()
            await self._producer.start()
            return self._producer

    async def _ensure_consumer_started(self) -> AIOKafkaConsumer:
        async with self._start_lock:
            if self._consumer is not None:
                return self._consumer
            if self._consumer_factory is None:
                raise ValueError("consumer_factory is not set")
            self._consumer = self._consumer_factory()

            if TYPE_CHECKING:
                pre_configured: bool = (
                    self._consumer._subscription._subscription_type  # pyright: ignore[reportPrivateUsage]
                    != SubscriptionType.NONE
                )
            else:
                pre_configured: bool = False
                sub = getattr(self._consumer, "_subscription", None)
                if sub is not None:
                    sub = getattr(sub, "_subscription_type", None)
                    pre_configured = sub != SubscriptionType.NONE

            # Require consumer to be created without pre-configured topics
            if pre_configured:
                raise RuntimeError(
                    "AIOKafkaConsumer was created with topics/pattern or manual assignment. "
                    "KafkaBaseClient uses subscribe() based on ParserSpec.topics. "
                    "Create the consumer WITHOUT topics or assigned partitions."
                )

            await self._consumer.start()

            # Group-managed subscribe to topics derived from ParserSpec
            if self._subscription_topics:
                # Guard: group_id must be set for subscribe mode
                try:
                    gid = getattr(self._consumer, "_group_id", None)
                    if gid is None:
                        gid = getattr(self._consumer, "group_id", None)
                except Exception:
                    gid = None
                if not gid:
                    raise RuntimeError(
                        "group_id must be provided on AIOKafkaConsumer for subscribe() mode"
                    )
                try:
                    self._consumer.subscribe(  # pyright: ignore[reportUnknownMemberType]
                        sorted(self._subscription_topics),
                        listener=self.rebalance_listener,
                    )
                except Exception:
                    logger.exception("Failed to subscribe to topics")
                    raise

            self._consumer_task = asyncio.create_task(
                self._consume_loop(), name=f"{self.__class__.__name__}_loop"
            )
            return self._consumer

    # assign/auto-expand are removed in subscribe mode

    async def _maybe_commit(self) -> None:
        if self._consumer is None or not self.auto_commit:
            return
        self._since_commit += 1
        now = time.time()
        if (
            (every := self.auto_commit["every"]) is not None
            and self._since_commit >= every
        ) or (
            (interval_s := self.auto_commit["interval_s"]) is not None
            and (now - self._last_commit) >= interval_s
        ):
            try:
                await self._consumer.commit()  # pyright: ignore[reportUnknownMemberType]
                self._since_commit = 0
                self._last_commit = now
            except Exception:
                logger.exception("Commit failed")

    # Parsing + Dispatching
    def _parse_record(
        self, record: ConsumerRecord[bytes, bytes]
    ) -> tuple[list[tuple[object, Type[object]]], Optional[bytes]]:
        topic = record.topic
        specs = self._parsers_by_topic.get(topic)

        # (1) Extract correlation_id (before parsing)
        cid = None
        if self.correlation_from_record:
            try:
                cid = self.correlation_from_record(record, None)
            except Exception as ex:
                logger.exception(f"correlation_from_record(None) failed: {ex}")

        # (2) Parsing
        parsed_candidates: list[tuple[object, Type[object]]] = []
        if specs:
            for spec in specs:
                try:
                    obj = spec["parser"](record)
                    parsed_candidates.append((obj, spec["type"]))
                    if not cid and self.correlation_from_record:
                        try:
                            cid = self.correlation_from_record(record, obj)
                        except Exception:
                            pass
                except Exception as ex:
                    logger.exception(
                        f"Parser failed (topic={topic}, out={getattr(spec['type'], '__name__', spec['type'])}): {ex}"
                    )

        # (3) fallback: raw
        if not parsed_candidates:
            parsed_candidates.append((record, ConsumerRecord))

        return parsed_candidates, cid

    async def _consume_loop(self) -> None:
        backoff = self.backoff_min
        try:
            while True:
                try:
                    assert self._consumer is not None
                    rec: ConsumerRecord[bytes, bytes] = await self._consumer.getone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                    parsed_candidates, cid = self._parse_record(rec)
                    try:
                        await self._on_record(rec, parsed_candidates, cid)
                    except Exception:
                        logger.exception("_on_record failed")
                    await self._maybe_commit()
                    backoff = self.backoff_min
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Unexpected error in consumer loop; will retry")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * self.backoff_factor, self.backoff_max)
        except asyncio.CancelledError:
            pass

    def _default_corr_from_record(
        self, rec: ConsumerRecord[bytes, bytes], parsed: Optional[object]
    ) -> Optional[bytes]:
        # Default correlation key extractor: Header priority, case-insensitive
        try:
            if rec.headers:
                for k, v in rec.headers:
                    if (k or "").lower() in self._corr_header_keys_lower:
                        try:
                            return bytes(v)
                        except Exception:
                            pass
        except Exception:
            pass
        try:
            if rec.key:
                return bytes(rec.key)
        except Exception:
            pass
        return None

    def assigned_table(self) -> list[dict[str, object]]:
        assigned: list[TopicPartition] = []
        try:
            if self._consumer is not None:
                current = self._consumer.assignment()  # pyright: ignore[reportUnknownMemberType]
                if current:
                    assigned = sorted(current, key=lambda x: (x.topic, x.partition))
        except Exception:
            assigned = []
        return [
            {
                "topic": tp.topic,
                "partition": tp.partition,
                "since": None,
                "source": "group",
                "seek_to_end_on_assign": self.seek_to_end_on_assign,
            }
            for tp in assigned
        ]

    @abstractmethod
    async def _on_record(
        self,
        record: ConsumerRecord[bytes, bytes],
        parsed_candidates: list[tuple[object, Type[object]]],
        cid: Optional[bytes],
    ) -> None: ...

    @abstractmethod
    async def _on_stop_cleanup(self) -> None: ...
