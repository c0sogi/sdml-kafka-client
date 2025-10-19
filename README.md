sdml-kafka-client
==================

Async Kafka client utilities for SDML built on aiokafka.

What's inside
-------------
- `KafkaListener`: subscribe to topics and stream parsed objects.
- `KafkaRPC`: request/response helper using correlation IDs over Kafka.
- Group-managed subscriptions only (subscribe-based). Manual assign is removed.

Requirements
------------
- Python >= 3.12
- Kafka cluster reachable from your app
- aiokafka >= 0.12.0

Install
-------
```bash
pip install sdml-kafka-client
```

Core concepts
-------------
- Group-managed subscribe: Consumers must have a non-empty `group_id` to join a consumer group. Kafka partitions are assigned and rebalanced by the coordinator.
- group_id strategy:
  - Same `group_id` among instances → competing consumers (load-balancing, each record processed once by the group).
  - Different `group_id` among instances → each instance receives the full stream (broadcast-style consumption).
  - Never share `group_id` between logically different roles (e.g., RPC clients vs RPC servers).

ParserSpec
----------
You declare which topics a client parses and how to parse them.

```python
from sdml.kafka_client.types import ParserSpec
from aiokafka import ConsumerRecord

def parse_json(rec: ConsumerRecord[bytes, bytes]) -> dict:
    import json
    return json.loads(rec.value or b"{}")

spec: ParserSpec[dict] = {
    "topics": ["events"],
    "type": dict,
    "parser": parse_json,
}
```

KafkaListener quickstart
------------------------
```python
import asyncio
from sdml.kafka_client.clients import KafkaListener
from sdml.kafka_client.types import ParserSpec

specs: list[ParserSpec[dict]] = [
    {
        "topics": ["events"],
        "type": dict,
        "parser": lambda r: {"topic": r.topic, "value": (r.value or b"").decode("utf-8")},
    }
]

listener = KafkaListener(
    parsers=specs,
    auto_commit={"every": 100, "interval_s": 5.0},
    consumer_factory=lambda: __import__("aiokafka").AIOKafkaConsumer(
        bootstrap_servers="127.0.0.1:9092",
        group_id="listener",              # required
        auto_offset_reset="latest",
    ),
)

async def main() -> None:
    await listener.start()
    stream = await listener.subscribe(dict)
    async for item in stream:
        print("got:", item)

asyncio.run(main())
```

KafkaRPC quickstart
-------------------
```python
import asyncio
from sdml.kafka_client.clients import KafkaRPC
from sdml.kafka_client.types import ParserSpec
from aiokafka import ConsumerRecord

def parse_reply(rec: ConsumerRecord[bytes, bytes]) -> bytes:
    return rec.value or b""

rpc = KafkaRPC(
    parsers=[{"topics": ["reply"], "type": bytes, "parser": parse_reply}],
    consumer_factory=lambda: __import__("aiokafka").AIOKafkaConsumer(
        bootstrap_servers="127.0.0.1:9092",
        group_id="rpc-client-unique",     # must be unique per requester instance
        auto_offset_reset="latest",
    ),
)

async def main() -> None:
    await rpc.start()
    res = await rpc.request(
        req_topic="request",
        req_value=b"hello",
        # Optionally direct server to respond to specific topics
        req_headers_reply_to=["reply"],
        res_expect_type=bytes,
    )
    print("response:", res)

asyncio.run(main())
```

RPC server pattern
------------------
Typical layout: servers consume from `request` and produce to reply topics passed in headers.

Guidelines:
- Server instances should share the same `group_id` to load-balance requests.
- Servers must NOT share `group_id` with clients.
- Server reads request, extracts `x-reply-topic` headers, and produces the response to that topic. If multiple reply topics are present, produce to each (or choose policy).

Group_id guidance
-----------------
- Listener
  - Same group_id → scale-out (each record processed once by the group).
  - Different group_id → broadcast (each listener gets all records).
- RPC server (responders)
  - Same group_id among servers → load-balancing for requests.
  - Different group_id among servers → all servers handle each request (usually wrong for RPC).
- RPC client (requesters)
  - Each client should have a unique group_id to avoid competing on replies.
  - Do not reuse server group_id.

Offsets & auto commit
---------------------
- `auto_commit` is optional. When provided, commits happen by message count and/or interval.
- `auto_offset_reset` defaults are set on consumer factories in examples to `latest`.

Correlation IDs
---------------
- `KafkaBaseClient` extracts correlation IDs from headers (`request_id`, `correlation_id`, `x-correlation-id`) or from the key when present.
- `KafkaRPC.request` can propagate the correlation ID in key and/or a header you choose (default header: `request_id`).

Production notes
----------------
- Always set explicit `group_id` in your provided `consumer_factory`.
- Use dedicated topics for requests and for replies. Avoid sending replies to the request topic.
- Isolate roles with different `group_id`s (clients vs servers).
- Ensure idempotency in servers when necessary.

API reference (selected)
------------------------
- `KafkaListener(parsers: Iterable[ParserSpec[object]], ...)`
  - `subscribe(tp: Type[T], queue_maxsize: int = 0, fresh: bool = False) -> TypeStream[T]`
- `KafkaRPC(parsers: Iterable[ParserSpec[object]], ...)`
  - `request(req_topic: str, req_value: bytes, *, req_key: bytes | None = None, req_headers: list[tuple[str, bytes]] | None = None, req_headers_reply_to: list[str] | None = None, res_timeout: float = 30.0, res_expect_type: Type[T] | None = None, correlation_id: bytes | None = None, propagate_corr_to: str = "both", correlation_header_key: str = "request_id") -> T`

License
-------
MIT


