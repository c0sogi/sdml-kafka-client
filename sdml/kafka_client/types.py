import asyncio
import contextlib
from typing import (
    Callable,
    Generic,
    NamedTuple,
    Optional,
    Self,
    Type,
    TypedDict,
    TypeVar,
)

from aiokafka import ConsumerRecord  # pyright: ignore[reportMissingTypeStubs]

T = TypeVar("T", covariant=True)


class ParserSpec(TypedDict, Generic[T]):
    """Specify the parser and the range of Kafka input (consume) in one go"""

    topics: list[str]
    type: Type[T]
    parser: Callable[[ConsumerRecord[bytes, bytes]], T]


class Waiter(NamedTuple, Generic[T]):
    future: asyncio.Future[T]
    expect_type: Optional[Type[T]]


class TypeStream(Generic[T]):
    def __init__(self, q: asyncio.Queue[T], event: asyncio.Event) -> None:
        self._q = q
        self._event = event

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        # If the event is set, stop the iteration
        if self._event.is_set():
            raise StopAsyncIteration

        get_task = asyncio.create_task(self._q.get())
        ev_task = asyncio.create_task(self._event.wait())
        try:
            done, _ = await asyncio.wait(
                {get_task, ev_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            # If the event is completed first (or at the same time), stop the iteration
            if ev_task in done and ev_task.result() is True:
                get_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await get_task
                raise StopAsyncIteration

            # If the item is received first, return it
            ev_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ev_task
            return get_task.result()

        finally:
            # Prevent leaks: clean up remaining tasks
            for t in (get_task, ev_task):
                if not t.done():
                    t.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await t


class AutoCommitConfig(TypedDict):
    every: int | None
    interval_s: float | None
