import asyncio
import time
import uuid
from dataclasses import dataclass
from types import TracebackType
from typing import Callable, Generic, Iterable, Optional, Self, Type, TypeVar

from aiokafka import (  # pyright: ignore[reportMissingTypeStubs]
    AIOKafkaConsumer,
    AIOKafkaProducer,
    ConsumerRecord,
    TopicPartition,
)
from beartype.door import is_bearable
from loguru import logger

T = TypeVar("T", covariant=True)

# 파서: ConsumerRecord -> 임의의 파싱 결과(여러 타입 허용)
Parser = Callable[[ConsumerRecord[bytes, bytes]], T]
# 상관키 추출기: (record, parsed or None) -> correlation_id (없으면 None)
CorrelationFromRecord = Callable[
    [ConsumerRecord[bytes, bytes], Optional[object]], Optional[bytes]
]

# 타입 스트림 종료 신호(데이터 None과 구분되는 전용 센티넬)
_SENTINEL = object()

# 헤더에서 상관키로 인식할 후보 키들(다양한 시스템 호환)
_DEFAULT_CORR_HEADER_KEYS: tuple[str, ...] = (
    "request_id",
    "correlation_id",
    "x-correlation-id",
)


@dataclass(frozen=True)
class ParserSpec(Generic[T]):
    topics: tuple[str, ...]  # 이 파서를 적용할 토픽들
    out_type: Type[T]  # 파서가 생산하는 결과 타입(런타임 체크 가능한 클래스형 권장)
    func: Parser[T]  # 실제 파서 함수


def default_corr_from_record(
    rec: ConsumerRecord[bytes, bytes], parsed: Optional[object]
) -> Optional[bytes]:
    """
    기본 상관키 추출:
    1) 레코드 key (bytes -> utf-8)
    2) 유명 헤더(request_id, correlation_id, x-correlation-id)
    """
    # Key
    if rec.key:
        try:
            return bytes(rec.key)
        except Exception:
            pass

    # Headers
    if rec.headers:
        for k, v in rec.headers:
            if k in _DEFAULT_CORR_HEADER_KEYS:
                try:
                    return bytes(v)
                except Exception:
                    pass

    return None


@dataclass
class KafkaClient:
    """
    Kafka RPC + 타입 기반 소비 유틸리티.

    - 여러 파서를 토픽별로 등록하고, 결과 타입(out_type)으로 소비/필터링.
    - 모드
      (1) 요청/응답(request): correlation id로 매칭 → expect_type으로 타입 필터
      (2) 단방향 소비(subscribe_types): 원하는 타입만 큐로 전달
    - 메모리 누수 방지: waiter/큐/태스크 정리 철저
    - 실전 안전성:
      * corr_id를 항상 메시지에 자동 부착(key & header) → 매칭 실패/타임아웃 방지
      * 파서 전부 실패 시 raw ConsumerRecord fallback
      * 타입 스트림 종료 시 전용 센티넬 사용(데이터 None과 구분)
      * 메타데이터 갱신 쓰로틀링
      * (옵션) 수동 커밋 배치 지원
    """

    # ---------- Producer ----------
    bootstrap_servers: str
    compression: Optional[str] = "gzip"
    linger_ms: int = 0
    request_timeout_ms: int = 5000
    metadata_max_age_ms: int = 60000
    api_version: str = "auto"

    # ---------- Consumer ----------
    group_id: Optional[str] = None
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = False
    fetch_max_wait_ms: int = 500
    fetch_max_bytes: int = 50 * 1024 * 1024
    max_partition_fetch_bytes: int = 50 * 1024 * 1024
    fetch_min_bytes: int = 1

    # ---------- Behavior ----------
    lazy_consumer_start: bool = True
    lazy_producer_start: bool = True
    seek_to_end_on_assign: bool = True  # 새 메시지부터만
    metadata_refresh_min_interval_s: float = 5.0  # 메타데이터 갱신 쓰로틀(초)
    # (옵션) 수동 커밋 배치: enable_auto_commit=False일 때만 의미 있음
    commit_on_consume: bool = False
    commit_every: int = 200
    commit_interval_s: float = 5.0

    # ---------- Parser ----------
    parsers: Iterable[ParserSpec[object]] = ()
    correlation_from_record: Optional[CorrelationFromRecord] = default_corr_from_record

    def __post_init__(self) -> None:
        # 내부 상태
        self._producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        self._consumer_task: Optional[asyncio.Task[None]] = None
        self._closed: bool = True

        # correlation_id -> _Waiter
        self._waiters: dict[bytes, _Waiter[object]] = {}

        # 타입별 스트림 큐 (consume-only)
        self._type_streams: dict[Type[object], asyncio.Queue[object]] = {}

        # 토픽별 파서 목록
        self._parsers_by_topic: dict[str, list[ParserSpec[object]]] = {}
        for spec in self.parsers:
            for t in spec.topics:
                self._parsers_by_topic.setdefault(t, []).append(spec)

        # 현재 assign된 파티션 셋
        self._assigned: set[TopicPartition] = set()

        # 메타데이터 갱신 쓰로틀링
        self._last_md_refresh: float = 0.0

        # 커밋 배치 상태
        self._since_commit: int = 0
        self._last_commit: float = time.time()

    # ---------------- Lifecycle ----------------
    async def start(self) -> None:
        if not self._closed:
            return

        if not self.lazy_producer_start:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                compression_type=self.compression,
                linger_ms=self.linger_ms,
                request_timeout_ms=self.request_timeout_ms,
                metadata_max_age_ms=self.metadata_max_age_ms,
                api_version=self.api_version,
            )
            await self._producer.start()

        if not self.lazy_consumer_start:
            await self._ensure_consumer_started()

        self._closed = False
        logger.info("KafkaClient started")

    async def stop(self) -> None:
        if self._closed:
            return

        # 소비 루프 태스크 중지
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
            self._consumer_task = None

        # 컨슈머 중지
        if self.consumer is not None:
            try:
                await self.consumer.stop()
            except Exception:
                logger.exception("Error stopping consumer")
            self.consumer = None

        # 프로듀서 중지
        if self._producer is not None:
            try:
                await self._producer.stop()
            except Exception:
                logger.exception("Error stopping producer")
            self._producer = None

        # 대기중 waiter 정리(누수 방지)
        for w in self._waiters.values():
            if not w.future.done():
                w.future.set_exception(RuntimeError("Client stopped before response"))
        self._waiters.clear()

        # 타입 스트림 종료 시그널(전용 센티넬 사용)
        for q in self._type_streams.values():
            try:
                q.put_nowait(_SENTINEL)
            except Exception:
                pass
        self._type_streams.clear()

        self._assigned.clear()
        self._closed = True
        logger.info("KafkaClient stopped")

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

    def register_parser(
        self, topics: Iterable[str], out_type: Type[T], func: Parser[T]
    ) -> None:
        """
        런타임 체크 가능한 클래스(out_type)만 등록하는 것을 권장.
        """
        spec = ParserSpec[object](tuple(topics), out_type, func)
        for t in topics:
            self._parsers_by_topic.setdefault(t, []).append(spec)

    # ---------------- 요청/응답 (RPC) ----------------
    async def request(
        self,
        *,
        req_topic: str,
        value: bytes,
        res_topic: str,
        response_partition: Optional[int] = None,
        key: Optional[bytes] = None,
        headers: Optional[list[tuple[str, bytes]]] = None,
        timeout: float = 30.0,
        expect_type: Optional[Type[T]] = None,  # 원하는 타입으로만 완료
        correlation_id: Optional[bytes] = None,
        propagate_corr_to: str = "both",  # "key" | "header" | "both"
        correlation_header_key: str = "request_id",  # 헤더 키명
    ) -> T:
        """
        요청을 게시하고 res_topic에서 correlation_id 매칭된 응답을 기다린다.
        - expect_type을 지정하면 해당 타입으로 파싱된 결과만 반환한다.
        - 파서가 없고 expect_type이 None인 경우, raw ConsumerRecord를 반환한다.
        - corr_id는 기본적으로 key/헤더에 자동 부착되어 매칭 실패를 방지한다.
        """
        if self._closed:
            await self.start()

        # Producer 준비
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                compression_type=self.compression,
                linger_ms=self.linger_ms,
                request_timeout_ms=self.request_timeout_ms,
                metadata_max_age_ms=self.metadata_max_age_ms,
                api_version=self.api_version,
            )
            await self._producer.start()

        # Consumer 준비 및 응답 토픽 할당
        await self._ensure_consumer_started()
        await self._assign_if_needed([(res_topic, response_partition)])

        # corr_id 생성/결정
        if correlation_id:
            corr_id = correlation_id
        elif key:
            corr_id = key
        else:
            corr_id = uuid.uuid4().hex.encode("utf-8")
        corr_id = bytes(corr_id)

        # corr_id를 항상 메시지에 전파(키/헤더)
        msg_key = key
        msg_headers = list(headers or [])
        if propagate_corr_to in ("key", "both") and msg_key is None:
            msg_key = corr_id
        if propagate_corr_to in ("header", "both"):
            if not any(k == correlation_header_key for k, _ in msg_headers):
                msg_headers.append((correlation_header_key, corr_id))

        # waiter 등록
        fut: asyncio.Future[T] = asyncio.get_event_loop().create_future()
        self._waiters[corr_id] = _Waiter[T](future=fut, expect_type=expect_type)

        try:
            await self._producer.send_and_wait(  # pyright: ignore[reportUnknownMemberType]
                req_topic,
                value=value,
                key=msg_key,
                headers=msg_headers,
            )
            logger.debug(f"sent request corr_id={corr_id} topic={req_topic}")

            return await asyncio.wait_for(fut, timeout=timeout)

        except asyncio.TimeoutError:
            self._waiters.pop(corr_id, None)  # 누수 방지
            raise TimeoutError(f"Timed out waiting for response (corr_id={corr_id})")
        except Exception:
            self._waiters.pop(corr_id, None)
            raise

    # ---------------- 단방향 소비 (타입 기반) ----------------
    async def subscribe_types(
        self,
        types: Iterable[Type[object]],
        *,
        queue_maxsize: int = 0,
        topics: Optional[
            Iterable[str]
        ] = None,  # 지정 없으면 해당 타입을 생산하는 모든 토픽을 자동 assign
    ) -> dict[Type[object], "asyncio.Queue[object]"]:
        """
        지정 타입들만 받아볼 수 있는 큐를 반환한다.
        - 내부적으로 해당 타입을 생산하는 파서가 등록된 토픽들만 assign한다.
        - topics가 주어지면 그 토픽들만 assign하되, 등록 파서가 있어야 타입이 생산된다.
        - 종료 시에는 큐에서 `_SENTINEL`을 받게 된다.
        """
        if self._closed:
            await self.start()

        await self._ensure_consumer_started()

        wanted = set(types)
        # 큐 준비
        for tp in wanted:
            self._type_streams.setdefault(tp, asyncio.Queue(maxsize=queue_maxsize))

        # assign 대상 계산
        to_assign: list[tuple[str, Optional[int]]] = []
        if topics:
            for t in topics:
                if any(
                    ps.out_type in wanted for ps in self._parsers_by_topic.get(t, [])
                ):
                    to_assign.append((t, None))
        else:
            for t, specs in self._parsers_by_topic.items():
                if any(ps.out_type in wanted for ps in specs):
                    to_assign.append((t, None))

        await self._assign_if_needed(to_assign)
        return {tp: self._type_streams[tp] for tp in wanted}

    # ---------------- 내부: Consumer 루프 ----------------
    async def _consume_loop(self) -> None:
        try:
            while True:
                assert self.consumer is not None

                record: ConsumerRecord[bytes, bytes] = await self.consumer.getone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

                topic = record.topic
                specs = self._parsers_by_topic.get(topic, [])

                # 1) 상관키 후보: (record, None) 먼저
                cid = None
                if self.correlation_from_record:
                    try:
                        cid = self.correlation_from_record(record, None)
                    except Exception as ex:
                        logger.exception(f"correlation_from_record(None) failed: {ex}")

                # 2) 파싱 시도 (여러 파서 가능)
                parsed_candidates: list[tuple[object, Type[object]]] = []
                if specs:
                    for spec in specs:
                        try:
                            obj = spec.func(record)
                            parsed_candidates.append((obj, spec.out_type))
                            # 상관키가 아직 없고, 파싱 결과로 뽑을 수 있다면 한 번 더 시도
                            if not cid and self.correlation_from_record:
                                try:
                                    cid = self.correlation_from_record(record, obj)
                                except Exception:
                                    pass
                        except Exception as ex:
                            logger.exception(
                                f"Parser failed (topic={topic}, out={getattr(spec.out_type, '__name__', spec.out_type)}): {ex}"
                            )

                # ✅ 파서가 없거나 모두 실패하면 raw fallback
                if not parsed_candidates:
                    parsed_candidates.append((record, ConsumerRecord))

                # 3) 요청/응답 매칭 우선 처리
                handled = False
                if cid and cid in self._waiters:
                    waiter = self._waiters.pop(cid, None)
                    if waiter and not waiter.future.done():
                        expect = waiter.expect_type
                        if expect is None:
                            # 타입 제약 없음 → 첫 후보 반환
                            waiter.future.set_result(parsed_candidates[0][0])
                            handled = True
                        else:
                            # 기대 타입과 일치하는 후보 탐색
                            for obj, ot in parsed_candidates:
                                try:
                                    if is_bearable(obj, expect):  # pyright: ignore[reportArgumentType]
                                        waiter.future.set_result(obj)
                                        handled = True
                                        break
                                except Exception:
                                    # 타입 체크 중 문제 나도 다음 후보 탐색
                                    pass
                            if not handled:
                                waiter.future.set_exception(
                                    TypeError(
                                        f"Response type mismatch: expected {str(expect)}, "
                                        f"got [{', '.join(str(ot) for _, ot in parsed_candidates)}]"
                                    )
                                )
                                handled = True  # waiter에 결과/예외를 이미 전달

                if handled:
                    await self._maybe_commit()
                    continue

                # 4) consume-only: 타입 구독자에게만 전달
                #    (여러 타입을 생산할 수 있으므로 모든 후보를 각각 해당 큐에 push)
                for obj, ot in parsed_candidates:
                    q = self._type_streams.get(ot)
                    if q:
                        try:
                            q.put_nowait(obj)
                        except asyncio.QueueFull:
                            # 간단한 백프레셔: 오래된 항목 한 개 드롭 후 push(최신성 우선)
                            try:
                                _ = q.get_nowait()
                                q.put_nowait(obj)
                            except Exception:
                                pass

                await self._maybe_commit()

        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Unexpected error in consumer loop")

    # ---------------- 내부: Consumer 시작/할당 ----------------
    async def _ensure_consumer_started(self) -> None:
        if self.consumer is not None:
            return
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=self.enable_auto_commit,
            auto_offset_reset=self.auto_offset_reset,
            fetch_max_bytes=self.fetch_max_bytes,
            max_partition_fetch_bytes=self.max_partition_fetch_bytes,
            fetch_max_wait_ms=self.fetch_max_wait_ms,
            fetch_min_bytes=self.fetch_min_bytes,
        )
        await self.consumer.start()
        self._consumer_task = asyncio.create_task(
            self._consume_loop(), name="kafka_consumer_loop"
        )

    async def _assign_if_needed(
        self, topic_partitions: Iterable[tuple[str, Optional[int]]]
    ) -> None:
        """
        단순 assign 기반 할당.
        - 특정 파티션 지정이 필요하거나, subscribe의 리밸런싱이 불필요한 시나리오에 적합.
        """
        assert self.consumer is not None
        new_tps: list[TopicPartition] = []

        # 메타데이터 갱신(쓰로틀)
        now = time.time()

        def _should_refresh() -> bool:
            return (now - self._last_md_refresh) >= self.metadata_refresh_min_interval_s

        for topic, part in topic_partitions:
            if part is None:
                # 공개 API로 메타데이터 갱신
                if _should_refresh():
                    try:
                        await (
                            self.consumer.topics()
                        )  # fetch_all_metadata()를 내부에서 호출
                        self._last_md_refresh = now
                    except Exception:
                        logger.exception(
                            f"Failed to refresh metadata for topic={topic}"
                        )
                        # 계속 진행(이후 partitions_for_topic에서 None일 수 있음)

                parts = self.consumer.partitions_for_topic(topic)  # pyright: ignore[reportUnknownMemberType]
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
        self.consumer.assign(all_tps)  # pyright: ignore[reportUnknownMemberType]
        self._assigned = set(all_tps)

        if self.seek_to_end_on_assign:
            for tp in new_tps:
                try:
                    await self.consumer.seek_to_end(tp)  # pyright: ignore[reportUnknownMemberType]
                except Exception:
                    logger.exception(f"seek_to_end failed for {tp}")

        logger.debug(
            f"Assigned partitions: {sorted(self._assigned, key=lambda x: (x.topic, x.partition))}"
        )

    # ---------------- 내부: 커밋 배치 ----------------
    async def _maybe_commit(self) -> None:
        if not self.commit_on_consume or self.enable_auto_commit:
            return
        assert self.consumer is not None
        self._since_commit += 1
        now = time.time()
        # 건수 또는 시간 기준 배치 커밋
        if (
            self._since_commit >= self.commit_every
            or (now - self._last_commit) >= self.commit_interval_s
        ):
            try:
                await self.consumer.commit()  # pyright: ignore[reportUnknownMemberType]
                self._since_commit = 0
                self._last_commit = now
            except Exception:
                logger.exception("Commit failed")


@dataclass
class _Waiter(Generic[T]):
    future: "asyncio.Future[T]"
    expect_type: Optional[Type[T]]  # 요청자가 원하는 결과 타입(없으면 아무거나 OK)
