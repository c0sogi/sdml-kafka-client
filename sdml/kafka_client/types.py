from asyncio import Future, Queue
from typing import (
    Callable,
    Generic,
    Iterable,
    NamedTuple,
    NotRequired,
    Optional,
    Self,
    Type,
    TypedDict,
    TypeVar,
)

from aiokafka import ConsumerRecord  # pyright: ignore[reportMissingTypeStubs]

T = TypeVar("T", covariant=True)


class AssignmentSpec(TypedDict):
    """파서가 처리할 카프카 입력(소비) 범위"""

    topic: str  # 필수
    partitions: NotRequired[Iterable[int]]  # 생략 시 전체 파티션


class ParserSpec(TypedDict, Generic[T]):
    """파서 + 소비 범위를 한 번에 명시"""

    assignments: Iterable[AssignmentSpec]
    type: Type[T]
    parser: Callable[[ConsumerRecord[bytes, bytes]], T]


class Waiter(NamedTuple, Generic[T]):
    future: Future[T]
    expect_type: Optional[Type[T]]


class TypeStream(Generic[T]):
    def __init__(self, q: Queue[object]) -> None:
        self._q = q

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        item = await self._q.get()

        if item is SENTINEL:
            raise StopAsyncIteration
        return item  # type: ignore[return-value]


SENTINEL = object()
