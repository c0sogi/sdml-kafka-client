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
from beartype.door import is_bearable
from loguru import logger

from ..types import T, Waiter
from .base_client import KafkaBaseClient


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
        super().__post_init__()
        self._producer_factory = producer_factory
        self._consumer_factory = consumer_factory
        self._waiters: dict[bytes, Waiter[object]] = {}

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

        # waiter
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
