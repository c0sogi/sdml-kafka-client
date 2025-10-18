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
from aiokafka.consumer.subscription_state import (  # pyright: ignore[reportMissingTypeStubs]
    SubscriptionType,
)
from loguru import logger

from ..types import AutoCommitConfig, ParserSpec


@dataclass
class KafkaBaseClient(ABC):
    """
    Static Assignment Mode:
    - 인스턴스 생성(=파서 등록) 시점에 '무엇을 들을지' 고정.
    - 이후 request()/subscribe()는 할당을 변경하지 않음.
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

    # ---------- Assignment ----------
    auto_expand_new_partitions: bool = False
    """Automatically expand new partitions in assignments where partitions are omitted"""

    def __post_init__(self) -> None:
        # 내부 상태
        self._producer_factory: Optional[Callable[[], AIOKafkaProducer]] = None
        self._consumer_factory: Optional[Callable[[], AIOKafkaConsumer]] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._consumer_task: Optional[asyncio.Task[None]] = None
        self._auto_expand_task: Optional[asyncio.Task[None]] = None
        self._closed: bool = True

        # 동시성 보호
        self._start_lock = asyncio.Lock()
        self._assign_lock = asyncio.Lock()

        # 파서 인덱스(토픽→파서 목록)
        self._parsers_by_topic: dict[str, list[ParserSpec[object]]] = {}

        # 파서 스펙으로부터 정적 할당 집계
        self._static_assign_tp_list: list[tuple[str, Optional[int]]] = []
        self._collect_parsers_and_assignments(self.parsers)

        # 현재 assign된 파티션 셋/메타
        self._assigned: set[TopicPartition] = set()
        self._assigned_since: dict[TopicPartition, float] = {}
        self._assigned_source: dict[TopicPartition, str] = {}

        # 쓰로틀/커밋 상태
        self._last_md_refresh: float = 0.0
        self._since_commit: int = 0
        self._last_commit: float = time.time()

        # corr-id 기본 추출기(대소문자 무시)
        self._corr_header_keys_lower = tuple(k.lower() for k in self.corr_header_keys)
        if self.correlation_from_record is None:
            self.correlation_from_record = self._default_corr_from_record

    # 파서 등록 + 정적 할당 집계(초기화 시 1회)
    def _collect_parsers_and_assignments(
        self, specs: Iterable[ParserSpec[object]]
    ) -> None:
        seen_tp: set[tuple[str, Optional[int]]] = set()
        for ps in specs:
            # 토픽 인덱스 구성
            for a in ps["assignments"]:
                topic = a["topic"]
                self._parsers_by_topic.setdefault(topic, []).append(ps)
            # 정적 할당 구성
            for a in ps["assignments"]:
                topic = a["topic"]
                parts = a.get("partitions", None)
                if parts is None:
                    if (topic, None) not in seen_tp:
                        self._static_assign_tp_list.append((topic, None))
                        seen_tp.add((topic, None))
                else:
                    for p in parts:
                        tp = (topic, int(p))
                        if tp not in seen_tp:
                            self._static_assign_tp_list.append((topic, int(p)))
                            seen_tp.add(tp)

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

        if self.auto_expand_new_partitions:
            self._maybe_start_auto_expand_task()

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

        if self._auto_expand_task:
            self._auto_expand_task.cancel()
            try:
                await self._auto_expand_task
            except asyncio.CancelledError:
                pass
            self._auto_expand_task = None

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

        self._assigned.clear()
        self._assigned_since.clear()
        self._assigned_source.clear()
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
                pre_assigned: bool = (
                    self._consumer._subscription._subscription_type  # pyright: ignore[reportPrivateUsage]
                    != SubscriptionType.NONE
                )
            else:
                pre_assigned: bool = False
                sub = getattr(self._consumer, "_subscription", None)
                if sub is not None:
                    sub = getattr(sub, "_subscription_type", None)
                    pre_assigned = sub != SubscriptionType.NONE
                    print(f"pre_assigned: {pre_assigned} {sub}")

            # 방어: 이미 subscribe 상태라면 assign과 충돌
            if pre_assigned:
                raise RuntimeError(
                    "AIOKafkaConsumer was created with topics/pattern (subscribe mode). "
                    "KafkaBaseClient uses assign() based on ParserSpec.assignments. "
                    "Create the consumer WITHOUT topics."
                )

            await self._consumer.start()

            if self._static_assign_tp_list and not self._assigned:
                await self._assign_if_needed(
                    self._static_assign_tp_list, source="static", force=True
                )

            self._consumer_task = asyncio.create_task(
                self._consume_loop(), name=f"{self.__class__.__name__}_loop"
            )
            return self._consumer

    async def _assign_if_needed(
        self,
        topic_partitions: Iterable[tuple[str, Optional[int]]],
        *,
        source: str,
        force: bool = False,
    ) -> None:
        if self._consumer is None:
            return

        async with self._assign_lock:
            new_tps: list[TopicPartition] = []
            now = time.time()

            def _should_refresh() -> bool:
                return (
                    now - self._last_md_refresh
                ) >= self.metadata_refresh_min_interval_s

            for topic, part in topic_partitions:
                if part is None:
                    if _should_refresh():
                        try:
                            await self._consumer.topics()  # fetch_all_metadata()
                            self._last_md_refresh = now
                        except Exception:
                            logger.exception(
                                f"Failed to refresh metadata for topic={topic}"
                            )

                    parts = self._consumer.partitions_for_topic(topic)  # pyright: ignore[reportUnknownMemberType]
                    if not parts:
                        logger.warning(f"Topic metadata not found or empty: {topic}")
                        continue
                    for p in parts:
                        tp = TopicPartition(topic, p)
                        if tp not in self._assigned:
                            new_tps.append(tp)
                else:
                    tp = TopicPartition(topic, part)
                    if tp not in self._assigned:
                        new_tps.append(tp)

            if not new_tps:
                return

            all_tps = list(self._assigned | set(new_tps))
            self._consumer.assign(all_tps)  # pyright: ignore[reportUnknownMemberType]

            for tp in new_tps:
                self._assigned.add(tp)
                self._assigned_since[tp] = time.time()
                self._assigned_source[tp] = source

            if self.seek_to_end_on_assign:
                for tp in new_tps:
                    try:
                        await self._consumer.seek_to_end(tp)  # pyright: ignore[reportUnknownMemberType]
                    except Exception:
                        logger.exception(f"seek_to_end failed for {tp}")

            logger.debug(
                f"Assigned partitions (added {len(new_tps)}): "
                f"{sorted(self._assigned, key=lambda x: (x.topic, x.partition))}"
            )

    def _maybe_start_auto_expand_task(self) -> None:
        # assignments에서 partitions 생략된 토픽이 하나라도 있어야 의미가 있다.
        if self._auto_expand_task:
            return
        topics_with_all_parts = {
            t for (t, p) in self._static_assign_tp_list if p is None
        }
        if not topics_with_all_parts:
            return

        async def _loop() -> None:
            backoff = self.metadata_refresh_min_interval_s
            try:
                while True:
                    await asyncio.sleep(backoff)
                    if self._consumer is None:
                        continue
                    try:
                        await self._consumer.topics()  # pyright: ignore[reportUnknownMemberType]
                        todo: list[tuple[str, Optional[int]]] = []
                        for topic in topics_with_all_parts:
                            parts = self._consumer.partitions_for_topic(topic)  # pyright: ignore[reportUnknownMemberType]
                            if not parts:
                                continue
                            for p in parts:
                                tp = TopicPartition(topic, p)
                                if tp not in self._assigned:
                                    todo.append((topic, p))
                        if todo:
                            await self._assign_if_needed(
                                todo, source="auto-expand", force=True
                            )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("Auto-expand loop error")
            except asyncio.CancelledError:
                pass

        self._auto_expand_task = asyncio.create_task(
            _loop(), name=f"{self.__class__.__name__}_auto_expand"
        )

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

    # ---------- 파싱 + 디스패치 ----------
    def _parse_record(
        self, record: ConsumerRecord[bytes, bytes]
    ) -> tuple[list[tuple[object, Type[object]]], Optional[bytes]]:
        topic = record.topic
        specs = self._parsers_by_topic.get(topic)

        # (1) corr-id 추출(파싱 전)
        cid = None
        if self.correlation_from_record:
            try:
                cid = self.correlation_from_record(record, None)
            except Exception as ex:
                logger.exception(f"correlation_from_record(None) failed: {ex}")

        # (2) 파싱
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

    # ---------- corr-id 기본 추출기(헤더 우선, 대소문자 무시) ----------
    def _default_corr_from_record(
        self, rec: ConsumerRecord[bytes, bytes], parsed: Optional[object]
    ) -> Optional[bytes]:
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

    # ---------- 진단 ----------
    def assigned_table(self) -> list[dict[str, object]]:
        return [
            {
                "topic": tp.topic,
                "partition": tp.partition,
                "since": self._assigned_since.get(tp),
                "source": self._assigned_source.get(tp, "static"),
                "seek_to_end_on_assign": self.seek_to_end_on_assign,
            }
            for tp in sorted(self._assigned, key=lambda x: (x.topic, x.partition))
        ]

    # ---------- 훅 ----------
    @abstractmethod
    async def _on_record(
        self,
        record: ConsumerRecord[bytes, bytes],
        parsed_candidates: list[tuple[object, Type[object]]],
        cid: Optional[bytes],
    ) -> None: ...

    @abstractmethod
    async def _on_stop_cleanup(self) -> None: ...
