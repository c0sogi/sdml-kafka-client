from typing import Callable, Generic, Iterable, Type, override

from aiokafka import (  # pyright: ignore[reportMissingTypeStubs]
    AIOKafkaConsumer,
    AIOKafkaProducer,
    ConsumerRecord,
)

from ..types import T
from .rpc import KafkaRPC


class KafkaSimpleRPC(KafkaRPC, Generic[T]):
    def __init__(
        self,
        bootstrap_servers: str,
        res_topic: str,
        res_expect_type: Type[T],
        res_parser: Callable[[ConsumerRecord[bytes, bytes]], T],
        *,
        res_partition: int = 0,
    ):
        super().__init__(
            producer_factory=lambda: AIOKafkaProducer(
                bootstrap_servers=bootstrap_servers
            ),
            consumer_factory=lambda: AIOKafkaConsumer(
                bootstrap_servers=bootstrap_servers
            ),
            parsers=[
                {
                    "assignments": [
                        {"topic": res_topic, "partitions": [res_partition]}
                    ],
                    "type": res_expect_type,
                    "parser": res_parser,
                }
            ],
        )
        self.res_topic: str = res_topic
        self.res_partition: int = res_partition
        self.res_expect_type: Type[T] = res_expect_type

    @override
    async def request(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        req_topic: str,
        req_value: bytes,
        *,
        req_partition: int | None = None,
        req_key: bytes | None = None,
        req_headers: list[tuple[str, bytes]] | None = None,
        req_headers_reply_to: Iterable[tuple[str, int | None]] | None = None,
        res_timeout: float = 30,
        res_expect_type: Type[T] | None = None,
        correlation_id: bytes | None = None,
        propagate_corr_to: str = "both",
        correlation_header_key: str = "request_id",
    ) -> T:
        return await super().request(
            req_key=req_key,
            req_value=req_value,
            req_topic=req_topic,
            req_partition=req_partition,
            req_headers_reply_to=[(self.res_topic, self.res_partition)],
            res_expect_type=self.res_expect_type,
            res_timeout=res_timeout,
        )
