import asyncio
from dataclasses import InitVar, dataclass
from typing import Callable, Optional, Type, TypeVar, cast

from aiokafka import (  # pyright: ignore[reportMissingTypeStubs]
    AIOKafkaConsumer,
    ConsumerRecord,
)

from ..types import T, TypeStream
from .base_client import KafkaBaseClient

V = TypeVar("V", covariant=True)


@dataclass
class KafkaListener(KafkaBaseClient):
    """A Kafka listener that subscribes to a topic and returns a stream of objects"""

    consumer_factory: InitVar[Callable[[], AIOKafkaConsumer]] = (
        lambda: AIOKafkaConsumer(bootstrap_servers="127.0.0.1:9092")
    )

    def __post_init__(self, consumer_factory: Callable[[], AIOKafkaConsumer]) -> None:
        super().__post_init__()
        self._consumer_factory = consumer_factory
        self._subscriptions: dict[
            Type[object], tuple[asyncio.Queue[object], asyncio.Event]
        ] = {}

    async def subscribe(
        self,
        tp: Type[T],
        *,
        queue_maxsize: int = 0,
        fresh: bool = False,
    ) -> TypeStream[T]:
        if self._closed:
            await self.start()
        await self._ensure_consumer_started()
        if fresh or tp not in self._subscriptions:
            # Replace with a completely new queue/event
            self._subscriptions[tp] = (
                asyncio.Queue(maxsize=queue_maxsize),
                asyncio.Event(),
            )
        q, event = self._subscriptions[tp]
        return TypeStream[T](cast(asyncio.Queue[T], q), event)

    async def _on_record(
        self,
        record: ConsumerRecord[bytes, bytes],
        parsed_candidates: list[tuple[object, Type[object]]],
        cid: Optional[bytes],
    ) -> None:
        for obj, ot in parsed_candidates:
            q_event = self._subscriptions.get(ot)
            if q_event is None:
                continue
            q, _event = q_event
            try:
                q.put_nowait(obj)
            except asyncio.QueueFull:
                try:
                    q.get_nowait()
                    q.put_nowait(obj)
                except Exception:
                    pass

    async def _on_stop_cleanup(self) -> None:
        for _q, event in self._subscriptions.values():
            try:
                event.set()
            except Exception:
                pass
        self._subscriptions.clear()
