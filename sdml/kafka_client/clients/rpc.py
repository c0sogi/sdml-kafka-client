import asyncio
import uuid
from dataclasses import InitVar, dataclass
from typing import (
    Callable,
    Optional,
    Type,
)

from aiokafka import (  # pyright: ignore[reportMissingTypeStubs]
    AIOKafkaConsumer,
    AIOKafkaProducer,
    ConsumerRecord,
)
from aiokafka.abc import ConsumerRebalanceListener  # pyright: ignore[reportMissingTypeStubs]
from aiokafka.structs import TopicPartition  # pyright: ignore[reportMissingTypeStubs]
from beartype.door import is_bearable
from loguru import logger

from ..types import T, Waiter
from .base_client import KafkaBaseClient


class _RPCRebalanceListener(ConsumerRebalanceListener):
    """Event-driven partition assignment notification for RPC consumer"""

    def __init__(self) -> None:
        self._assigned_event = asyncio.Event()

    def on_partitions_revoked(self, revoked: list[TopicPartition]) -> None:
        """Called before rebalance"""
        self._assigned_event.clear()

    def on_partitions_assigned(self, assigned: list[TopicPartition]) -> None:
        """Called after partition assignment - signal readiness"""
        if assigned:
            self._assigned_event.set()

    async def wait_for_assignment(self, timeout: float) -> bool:
        """Wait for partitions to be assigned, returns True if assigned"""
        try:
            await asyncio.wait_for(self._assigned_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False


@dataclass
class KafkaRPC(KafkaBaseClient):
    """
    - The range of response (receive): ParserSpec.topics is statically determined
    - The request (send) topic: specified once at request() call
    - Match responses by correlation_id
    """

    producer_factory: InitVar[Callable[[], AIOKafkaProducer]] = (
        lambda: AIOKafkaProducer(bootstrap_servers="127.0.0.1:9092")
    )
    consumer_factory: InitVar[Callable[[], AIOKafkaConsumer]] = (
        lambda: AIOKafkaConsumer(
            bootstrap_servers="127.0.0.1:9092",
            group_id=f"rpc-request-{uuid.uuid4().hex}",
            auto_offset_reset="latest",
        )
    )

    def __post_init__(
        self,
        producer_factory: Callable[[], AIOKafkaProducer],
        consumer_factory: Callable[[], AIOKafkaConsumer],
    ) -> None:
        # Set up rebalance listener BEFORE calling super().__post_init__()
        # so it's available when consumer subscribes
        self._rpc_rebalance_listener = _RPCRebalanceListener()
        self.rebalance_listener = self._rpc_rebalance_listener

        super().__post_init__()
        self._producer_factory = producer_factory
        self._consumer_factory = consumer_factory
        self._waiters: dict[bytes, Waiter[object]] = {}
        self._request_lock = asyncio.Lock()  # Serialize seek + send

    async def request(
        self,
        req_topic: str,
        req_value: bytes,
        *,
        req_key: Optional[bytes] = None,
        req_headers: Optional[list[tuple[str, bytes]]] = None,
        req_headers_reply_to: Optional[list[str]] = None,
        res_timeout: float = 30.0,
        res_expect_type: Optional[Type[T]] = None,
        # Correlation ID Resolution
        correlation_id: Optional[bytes] = None,
        propagate_corr_to: str = "both",
        correlation_header_key: str = "request_id",
    ) -> T:
        if not req_topic or not req_topic.strip():
            raise ValueError("req_topic must be non-empty")

        # corr-id
        if correlation_id:
            corr_id = correlation_id
        elif req_key:
            corr_id = req_key
        else:
            corr_id = uuid.uuid4().hex.encode("utf-8")
        corr_id = bytes(corr_id)

        # 전파
        msg_key = req_key
        msg_headers = list(req_headers or [])
        if propagate_corr_to in ("key", "both") and msg_key is None:
            msg_key = corr_id
        if propagate_corr_to in ("header", "both"):
            if not any(
                k.lower() == correlation_header_key.lower() for k, _ in msg_headers
            ):
                msg_headers.append((correlation_header_key, corr_id))

        if req_headers_reply_to:
            for topic in req_headers_reply_to:
                msg_headers.append(("x-reply-topic", topic.encode("utf-8")))

        # Start the connection
        if self._closed:
            await self.start()
        producer = await self._ensure_producer_started()
        await self._ensure_consumer_started()

        # Serialize seek + send to avoid race conditions with concurrent requests
        async with self._request_lock:
            # Wait for partition assignment using event-driven listener
            if self._consumer:
                assignment = self._consumer.assignment()  # pyright: ignore[reportUnknownMemberType]
                if not assignment:
                    # Wait for partition assignment event (event-driven, no polling!)
                    assigned = await self._rpc_rebalance_listener.wait_for_assignment(
                        self.assignment_timeout_s
                    )
                    if not assigned:
                        logger.warning(
                            f"Partition assignment timeout after {self.assignment_timeout_s}s, proceeding anyway"
                        )
                    assignment = self._consumer.assignment()  # pyright: ignore[reportUnknownMemberType]

                # Seek all assigned partitions to end to avoid reading old messages
                if assignment:
                    try:
                        await self._consumer.seek_to_end(*assignment)  # pyright: ignore[reportUnknownMemberType]
                    except Exception:
                        logger.exception("Failed to seek to end")

            # waiter (register before sending to avoid race)
            fut: asyncio.Future[T] = asyncio.get_event_loop().create_future()
            self._waiters[corr_id] = Waiter[T](future=fut, expect_type=res_expect_type)

            try:
                await producer.send_and_wait(  # pyright: ignore[reportUnknownMemberType]
                    req_topic,
                    value=req_value,
                    key=msg_key,
                    headers=msg_headers,
                )
                logger.debug(f"sent request corr_id={corr_id} topic={req_topic}")
            except Exception:
                self._waiters.pop(corr_id, None)
                raise

        # Wait for response (outside lock to allow other requests to queue)
        try:
            return await asyncio.wait_for(fut, timeout=res_timeout)

        except asyncio.TimeoutError:
            self._waiters.pop(corr_id, None)
            raise TimeoutError(f"Timed out waiting for response (corr_id={corr_id})")
        except Exception:
            self._waiters.pop(corr_id, None)
            raise

    async def _on_record(
        self,
        record: ConsumerRecord[bytes, bytes],
        parsed_candidates: list[tuple[object, Type[object]]],
        cid: Optional[bytes],
    ) -> None:
        if not cid:
            return
        waiter = self._waiters.get(cid)
        if not waiter or waiter.future.done():
            return

        expect = waiter.expect_type
        if expect is None:
            waiter.future.set_result(parsed_candidates[0][0])
            self._waiters.pop(cid, None)
            return

        for obj, _ in parsed_candidates:
            try:
                if is_bearable(obj, expect):  # pyright: ignore[reportArgumentType]
                    waiter.future.set_result(obj)
                    self._waiters.pop(cid, None)
                    return
            except Exception:
                pass

        logger.debug(
            f"Response type mismatch for corr_id={cid!r}: "
            f"expected {expect}, got [{', '.join(str(ot) for _, ot in parsed_candidates)}]"
        )

    async def _on_stop_cleanup(self) -> None:
        for w in self._waiters.values():
            if not w.future.done():
                w.future.set_exception(RuntimeError("Client stopped before response"))
        self._waiters.clear()
