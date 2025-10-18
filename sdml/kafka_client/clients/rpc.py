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
    - 응답(수신) 범위: ParserSpec.assignments로 정적 확정
    - 요청(송신) 토픽/파티션: request() 호출 시 단발 지정
    - corr-id로 응답 매칭
    """

    producer_factory: InitVar[Callable[[], AIOKafkaProducer]] = (
        lambda: AIOKafkaProducer(bootstrap_servers="127.0.0.1:9092")
    )
    consumer_factory: InitVar[Callable[[], AIOKafkaConsumer]] = (
        lambda: AIOKafkaConsumer(bootstrap_servers="127.0.0.1:9092")
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
        *,
        req_topic: str,
        value: bytes,
        req_partition: Optional[int] = None,
        key: Optional[bytes] = None,
        headers: Optional[list[tuple[str, bytes]]] = None,
        timeout: float = 30.0,
        expect_type: Optional[Type[T]] = None,
        correlation_id: Optional[bytes] = None,
        propagate_corr_to: str = "both",
        correlation_header_key: str = "request_id",
    ) -> T:
        if not req_topic or not req_topic.strip():
            raise ValueError("req_topic must be non-empty")

        # corr-id
        if correlation_id:
            corr_id = correlation_id
        elif key:
            corr_id = key
        else:
            corr_id = uuid.uuid4().hex.encode("utf-8")
        corr_id = bytes(corr_id)

        # 전파
        msg_key = key
        msg_headers = list(headers or [])
        if propagate_corr_to in ("key", "both") and msg_key is None:
            msg_key = corr_id
        if propagate_corr_to in ("header", "both"):
            if not any(
                k.lower() == correlation_header_key.lower() for k, _ in msg_headers
            ):
                msg_headers.append((correlation_header_key, corr_id))

        # 커넥션 시작
        if self._closed:
            await self.start()
        producer = await self._ensure_producer_started()
        await self._ensure_consumer_started()

        # waiter
        fut: asyncio.Future[T] = asyncio.get_event_loop().create_future()
        self._waiters[corr_id] = Waiter[T](future=fut, expect_type=expect_type)

        try:
            await producer.send_and_wait(  # pyright: ignore[reportUnknownMemberType]
                req_topic,
                value=value,
                key=msg_key,
                headers=msg_headers,
                partition=req_partition,
            )
            logger.debug(
                f"sent request corr_id={corr_id} topic={req_topic} partition={req_partition}"
            )

            return await asyncio.wait_for(fut, timeout=timeout)

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
            waiter.future.set_result(parsed_candidates[0][0])  # type: ignore[arg-type]
            self._waiters.pop(cid, None)
            return

        for obj, _ in parsed_candidates:
            try:
                if is_bearable(obj, expect):  # pyright: ignore[reportArgumentType]
                    waiter.future.set_result(obj)  # type: ignore[arg-type]
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
