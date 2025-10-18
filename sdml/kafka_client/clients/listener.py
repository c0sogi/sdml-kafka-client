import asyncio
from dataclasses import InitVar, dataclass
from typing import Callable, Optional, Type

from aiokafka import (  # pyright: ignore[reportMissingTypeStubs]
    AIOKafkaConsumer,
    ConsumerRecord,
)

from ..types import SENTINEL, T, TypeStream
from .base_client import KafkaBaseClient


@dataclass
class KafkaListener(KafkaBaseClient):
    """
    - 소비 범위는 ParserSpec.assignments로 정적 확정
    - subscribe(tp): 큐만 반환
    """

    consumer_factory: InitVar[Callable[[], AIOKafkaConsumer]] = (
        lambda: AIOKafkaConsumer(bootstrap_servers="127.0.0.1:9092")
    )

    def __post_init__(self, consumer_factory: Callable[[], AIOKafkaConsumer]) -> None:
        super().__post_init__()
        self._consumer_factory = consumer_factory
        self._type_queues: dict[Type[object], asyncio.Queue[object]] = {}

    async def subscribe(
        self,
        tp: Type[T],
        *,
        queue_maxsize: int = 0,
    ) -> TypeStream[T]:
        if self._closed:
            await self.start()
        await self._ensure_consumer_started()

        self._type_queues.setdefault(tp, asyncio.Queue(maxsize=queue_maxsize))
        return TypeStream(self._type_queues[tp])

    async def _on_record(
        self,
        record: ConsumerRecord[bytes, bytes],
        parsed_candidates: list[tuple[object, Type[object]]],
        cid: Optional[bytes],
    ) -> None:
        for obj, ot in parsed_candidates:
            q = self._type_queues.get(ot)
            if q:
                try:
                    q.put_nowait(obj)
                except asyncio.QueueFull:
                    try:
                        _ = q.get_nowait()
                        q.put_nowait(obj)
                    except Exception:
                        pass

    async def _on_stop_cleanup(self) -> None:
        for q in self._type_queues.values():
            try:
                q.put_nowait(SENTINEL)
            except Exception:
                pass
        self._type_queues.clear()
