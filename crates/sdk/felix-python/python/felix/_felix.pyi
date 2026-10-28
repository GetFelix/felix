"""Type stubs for the native module.

Kept by hand and checked against the binding by `tests/test_api_surface.py`,
so a signature cannot drift here without a test noticing.
"""

from types import TracebackType
from typing import AsyncIterator, Awaitable, Callable, Iterator, Literal, Sequence

__version__: str

AckMode = Literal["none", "per_message", "per_batch"]
StartPosition = Literal["latest", "earliest"] | int

RetryClass = Literal["retry", "retry_after", "redirect", "outcome_unknown", "fatal"]

class FelixError(Exception):
    code: str | None
    """The broker's error code, e.g. ``"shard_unavailable"``. ``None`` when
    the broker predates error codes or the failure was local."""
    retry: RetryClass | None
    """What the broker says the caller may do, or ``None`` without a code."""
    detail: dict[str, str | int] | None
    """Extra facts such as ``reason`` or ``retry_after_ms``, or ``None``."""

class ConnectionError(FelixError): ...
class AuthError(FelixError): ...
class NotFoundError(FelixError): ...
class CursorError(FelixError): ...
class ShardUnavailableError(FelixError): ...
class OverloadedError(FelixError): ...
class OutcomeUnknownError(FelixError): ...

class CommitError(FelixError):
    """A commit was refused before it was sent."""

class NotOnOwningShardError(CommitError):
    """An op names a stream other than the commit's: a second log."""

    index: int
    stream: str
    owner: str

class EventCountError(CommitError):
    """A commit carries exactly one event; this one had ``count``."""

    count: int

class PublishRefusedError(FelixError):
    """The broker would not append an idempotent publish. Nothing was written."""

    reason: Literal[
        "sequence_gap",
        "unknown_producer",
        "sequence_expired",
        "sequence_reused",
        "offset_mismatch",
        "not_leader",
    ]
    expected: int | None
    """For `sequence_gap`: the sequence the broker would have appended."""
    tail: int | None
    """For `offset_mismatch`: the shard's next offset."""
    node_id: str | None
    """For `not_leader`: the node that leads the shard."""
    addr: str | None
    """For `not_leader`: where that node serves clients, when known."""

class CommitOp:
    """One part of an atomic commit."""

    @staticmethod
    def publish(stream: str, payload: bytes) -> "CommitOp": ...
    @staticmethod
    def enqueue(queue: str, payload: bytes) -> "CommitOp": ...
    @staticmethod
    def put(stream: str, key: str, value: bytes) -> "CommitOp": ...
    @staticmethod
    def delete(stream: str, key: str) -> "CommitOp": ...

class CommitReceipt:
    offset: int

class StateValue:
    value: bytes | None
    version: int | None
    as_of: int | None

class CacheCondition:
    """What a conditional cache put requires of the key's current entry."""

    @staticmethod
    def absent() -> "CacheCondition": ...
    @staticmethod
    def version(version: int) -> "CacheCondition": ...

class CacheConditionResult:
    applied: bool
    version: int | None
    """The version written when applied; otherwise the key's current one, or None."""

class VersionedValue:
    value: bytes
    version: int

class ConditionalWrite:
    written: bool
    offset: int | None
    """Where the write landed; None when refused."""
    tail: int | None
    """The shard's next offset when refused; None when written."""

class GroupPosition:
    offset: int
    """The offset the group resumes from."""
    moved: bool
    """False when `group_create` found the group already there."""

class GroupInfo:
    committed: int | None
    """Everything below this is finished; None for a group with no cursor."""
    tail: int
    """The shard's committed tail."""
    in_flight: int
    owed: int
    dead_letters: int
    @property
    def lag(self) -> int: ...

class Producer:
    id: int
    closed: bool
    def publish(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        payload: bytes,
        *,
        key: bytes | None = None,
    ) -> int | None: ...
    def publish_batch(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        payloads: list[bytes],
        *,
        key: bytes | None = None,
    ) -> int | None: ...
    def publish_batches(
        self, tenant_id: str, namespace: str, stream: str, batches: list[list[bytes]]
    ) -> None: ...
    def abandon_in_doubt(self, tenant_id: str, namespace: str, stream: str) -> None: ...
    def id_for(self, tenant_id: str, namespace: str, stream: str) -> int: ...
    def close(self, timeout: float | None = None) -> None: ...
    def __enter__(self) -> "Producer": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: object | None = None,
    ) -> bool: ...

class AsyncProducer:
    id: int
    async def publish(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        payload: bytes,
        *,
        key: bytes | None = None,
    ) -> int | None: ...
    async def publish_batch(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        payloads: list[bytes],
        *,
        key: bytes | None = None,
    ) -> int | None: ...
    async def publish_batches(
        self, tenant_id: str, namespace: str, stream: str, batches: list[list[bytes]]
    ) -> None: ...
    async def abandon_in_doubt(self, tenant_id: str, namespace: str, stream: str) -> None: ...
    async def id_for(self, tenant_id: str, namespace: str, stream: str) -> int: ...
    async def close(self, timeout: float | None = None) -> None: ...
    @property
    async def closed(self) -> bool: ...
    async def __aenter__(self) -> "AsyncProducer": ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: object | None = None,
    ) -> bool: ...

class Event:
    tenant_id: str
    namespace: str
    stream: str
    payload: bytes
    offset: int | None
    skipped_before: int

class SubscriptionHandle:
    closed: bool
    start_offset: int | None
    """Where the subscription began, as of when it opened; None for an in-memory stream."""
    live_offset: int | None
    """The offset live delivery began at, as of when it opened."""
    def next_event(self, timeout: float | None = None) -> Event | None: ...
    def close(self) -> None: ...
    def __iter__(self) -> Iterator[Event]: ...
    def __next__(self) -> Event: ...
    def __enter__(self) -> "SubscriptionHandle": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...


# ---------------------------------------------------------------------------
# Queues, cache watches, and multi-shard streams
# ---------------------------------------------------------------------------

class GroupRecord:
    """One record claimed by a consumer group member."""

    offset: int
    payload: bytes
    attempts: int
    skipped_before: int
    """Offsets directly below this one settled without delivery; such a hole will not fill."""

class CacheChange:
    key: str
    value: bytes | None
    offset: int
    expires_at_millis: int

class CacheWatchLagged:
    """The watch fell behind; re-watch from ``resume_from`` for no gap."""

    resume_from: int

class CacheWatchShardMoved:
    """The watch's shard moved to another broker.

    A notice, not an end: the watch follows the shard and carries on.
    """

    resume_from: int | None
    node_id: str | None
    addr: str | None
    generation: int

WatchItem = CacheChange | CacheWatchLagged | CacheWatchShardMoved

class CacheWatchFilter:
    @staticmethod
    def key(value: str) -> "CacheWatchFilter": ...
    @staticmethod
    def prefix(value: str) -> "CacheWatchFilter": ...

class ShardRecord:
    shard: int
    event: Event

class ShardLost:
    shard: int
    error: str

class ShardRecovered:
    shard: int

class ShardMoved:
    """A shard moved to another broker; the subscription follows it."""

    shard: int
    resume_from: int | None
    node_id: str | None
    addr: str | None
    generation: int

class StreamDeleted:
    """The stream was deleted; this shard's feed ended and is not re-established."""

    shard: int

ShardEvent = ShardRecord | ShardLost | ShardRecovered | ShardMoved | StreamDeleted

class CacheWatchHandle:
    resume_offset: int
    resnapshot: bool
    retained_count: int | None
    closed: bool
    def recv(self, timeout: float | None = None) -> WatchItem | None: ...
    def close(self) -> None: ...
    def __iter__(self) -> Iterator[WatchItem]: ...
    def __next__(self) -> WatchItem: ...
    def __enter__(self) -> "CacheWatchHandle": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...

class ShardedSubscriptionHandle:
    shards: int
    closed: bool
    def next_event(self, timeout: float | None = None) -> ShardEvent | None: ...
    def positions(self) -> dict[int, int]: ...
    def close(self) -> None: ...
    def __iter__(self) -> Iterator[ShardEvent]: ...
    def __next__(self) -> ShardEvent: ...
    def __enter__(self) -> "ShardedSubscriptionHandle": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...

class AsyncCacheWatch:
    resume_offset: int
    resnapshot: bool
    retained_count: int | None
    async def recv(self, timeout: float | None = None) -> WatchItem | None: ...
    async def close(self) -> None: ...
    def __aiter__(self) -> AsyncIterator[WatchItem]: ...
    async def __anext__(self) -> WatchItem: ...
    async def __aenter__(self) -> "AsyncCacheWatch": ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...

class AsyncShardedSubscription:
    shards: int
    async def next_event(self, timeout: float | None = None) -> ShardEvent | None: ...
    async def positions(self) -> dict[int, int]: ...
    async def close(self) -> None: ...
    def __aiter__(self) -> AsyncIterator[ShardEvent]: ...
    async def __anext__(self) -> ShardEvent: ...
    async def __aenter__(self) -> "AsyncShardedSubscription": ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...

class StreamRecord:
    offset: int
    payload: bytes
    publisher: str | None
    """The principal that published the record, when the broker recorded one."""
    timestamp_micros: int
    """When the record was appended, in microseconds since the Unix epoch."""

class StreamPage:
    records: list[StreamRecord]
    next_offset: int
    """Where the next page starts; offsets holding no record are passed over."""

class ShardPublish:
    shard: int
    records: list[int]
    """Positions, in the order passed, of the records in this piece."""
    offset: int | None
    error: FelixError | None
    """Why the piece was not acknowledged; None when it was."""

class BatchReport:
    shards: list[ShardPublish]
    @property
    def ok(self) -> bool: ...
    def failed_records(self) -> list[int]: ...

class ShardedGroupRecord:
    shard: int
    record: GroupRecord

class ShardedGroupHandle:
    shards: int
    def poll(self, *, max_records: int = 32) -> list[ShardedGroupRecord]: ...
    def ack(self, record: ShardedGroupRecord) -> None: ...
    def nack(self, record: ShardedGroupRecord) -> None: ...
    def nack_after(self, record: ShardedGroupRecord, delay: float) -> None: ...
    def extend(self, record: ShardedGroupRecord, extend: float) -> float: ...
    def dead_letter(self, record: ShardedGroupRecord) -> None: ...

class AsyncShardedGroup:
    shards: int
    async def poll(self, *, max_records: int = 32) -> list[ShardedGroupRecord]: ...
    async def ack(self, record: ShardedGroupRecord) -> None: ...
    async def nack(self, record: ShardedGroupRecord) -> None: ...
    async def nack_after(self, record: ShardedGroupRecord, delay: float) -> None: ...
    async def extend(self, record: ShardedGroupRecord, extend: float) -> float: ...
    async def dead_letter(self, record: ShardedGroupRecord) -> None: ...

class ShardedCacheChange:
    shard: int
    change: CacheChange

class CacheStateComplete:
    """Every shard has delivered its state; everything further is live."""

class CacheShardLagged:
    """One shard fell behind and its watch ended; others are unaffected."""

    shard: int
    resume_from: int

class CacheShardClosed:
    """One shard's watch ended without a lag signal; others are unaffected."""

    shard: int

ShardedWatchItem = (
    ShardedCacheChange | CacheStateComplete | CacheShardLagged | ShardMoved | CacheShardClosed
)

class ShardedCacheWatchHandle:
    shards: int
    retained_count: int | None
    closed: bool
    def recv(self, timeout: float | None = None) -> ShardedWatchItem | None: ...
    def resume_offsets(self) -> dict[int, int]: ...
    def close(self) -> None: ...
    def __iter__(self) -> Iterator[ShardedWatchItem]: ...
    def __next__(self) -> ShardedWatchItem: ...
    def __enter__(self) -> "ShardedCacheWatchHandle": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...

class AsyncShardedCacheWatch:
    shards: int
    retained_count: int | None
    async def recv(self, timeout: float | None = None) -> ShardedWatchItem | None: ...
    async def resume_offsets(self) -> dict[int, int]: ...
    async def close(self) -> None: ...
    def __aiter__(self) -> AsyncIterator[ShardedWatchItem]: ...
    async def __anext__(self) -> ShardedWatchItem: ...
    async def __aenter__(self) -> "AsyncShardedCacheWatch": ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...

class Client:
    def __init__(
        self,
        addrs: str | Sequence[str],
        *,
        tenant_id: str,
        token: str | None = None,
        token_provider: Callable[[], str | Awaitable[str]] | None = None,
        server_name: str = "localhost",
        ca_file: str | None = None,
        offer_alpn: bool = False,
        reconnect_attempts: int | None = None,
        reconnect_deadline: float | None = None,
    ) -> None:
        """Pass ``token``, ``token_provider``, or both (``token`` is then the first token).

        A plain provider runs on a Felix worker thread with the GIL; an
        ``async def`` is run there with ``asyncio.run``. It cannot be cancelled: a
        call that outlives the 30 s fetch timeout fails the waiting operations and is
        joined by the next one, not repeated.
        """
    def with_identity(
        self,
        tenant_id: str,
        token: str | None = None,
        *,
        token_provider: Callable[[], str | Awaitable[str]] | None = None,
    ) -> "Client": ...
    def publish(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        payload: bytes,
        *,
        key: bytes | None = None,
        ack: AckMode = "per_message",
        at_least_once: bool = False,
    ) -> int | None: ...
    def publish_on_shard(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        payloads: list[bytes],
        *,
        ack: AckMode = "per_message",
    ) -> int | None: ...
    def producer(self) -> Producer: ...
    def subscribe(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        *,
        start: StartPosition | None = None,
        shard: int = 0,
    ) -> SubscriptionHandle: ...
    def cache_put(
        self,
        tenant_id: str,
        namespace: str,
        cache: str,
        key: str,
        value: bytes,
        *,
        ttl: float | None = None,
    ) -> None: ...
    def cache_get(
        self, tenant_id: str, namespace: str, cache: str, key: str
    ) -> bytes | None: ...
    def cache_delete(
        self, tenant_id: str, namespace: str, cache: str, key: str
    ) -> bytes | None: ...
    def commit(
        self, tenant_id: str, namespace: str, entity_key: bytes, ops: list[CommitOp]
    ) -> CommitReceipt: ...
    def state_get(
        self, tenant_id: str, namespace: str, stream: str, entity_key: bytes, key: str
    ) -> StateValue: ...
    def cache_get_versioned(
        self, tenant_id: str, namespace: str, cache: str, key: str
    ) -> VersionedValue | None: ...
    def cache_put_if(
        self,
        tenant_id: str,
        namespace: str,
        cache: str,
        key: str,
        value: bytes,
        condition: CacheCondition,
        *,
        ttl: float | None = None,
    ) -> CacheConditionResult: ...
    def cache_delete_if(
        self, tenant_id: str, namespace: str, cache: str, key: str, version: int
    ) -> CacheConditionResult: ...
    def commit_if(
        self,
        tenant_id: str,
        namespace: str,
        entity_key: bytes,
        ops: list[CommitOp],
        expected_offset: int,
    ) -> ConditionalWrite: ...
    def commit_on_shard(
        self,
        tenant_id: str,
        namespace: str,
        shard: int,
        entity_key: bytes,
        ops: list[CommitOp],
    ) -> CommitReceipt: ...
    def commit_if_on_shard(
        self,
        tenant_id: str,
        namespace: str,
        shard: int,
        entity_key: bytes,
        ops: list[CommitOp],
        expected_offset: int,
    ) -> ConditionalWrite: ...
    def state_get_on_shard(
        self, tenant_id: str, namespace: str, stream: str, shard: int, key: str
    ) -> StateValue: ...
    def publish_if(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        payloads: list[bytes],
        expected_offset: int,
        *,
        key: bytes | None = None,
    ) -> ConditionalWrite: ...
    def publish_if_on_shard(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        payloads: list[bytes],
        expected_offset: int,
    ) -> ConditionalWrite: ...
    def counter_add(
        self, tenant_id: str, namespace: str, cache: str, key: str, delta: int
    ) -> int: ...
    def counter_get(
        self, tenant_id: str, namespace: str, cache: str, key: str
    ) -> int | None: ...
    def group_poll(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        group: str,
        *,
        max_records: int = 32,
        wait: float = 5.0,
        consumer: str | None = None,
        reclaim: bool = False,
        visibility: float | None = None,
    ) -> list[GroupRecord]: ...
    def group_ack(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str, offset: int
    ) -> None: ...
    def group_nack(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str, record: GroupRecord
    ) -> None: ...
    def group_dead_letters(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str
    ) -> list[int]: ...
    def group_discard(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str, offset: int
    ) -> None: ...
    def group_redrive(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str, offset: int
    ) -> None: ...
    def group_extend(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        group: str,
        record: GroupRecord,
        extend: float,
    ) -> float: ...
    def group_seek(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        group: str,
        start: StartPosition,
    ) -> GroupPosition: ...
    def group_create(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        group: str,
        start: StartPosition,
    ) -> GroupPosition: ...
    def group_describe(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str
    ) -> GroupInfo: ...
    def group_delete(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str
    ) -> bool: ...
    def watch_cache(
        self,
        tenant_id: str,
        namespace: str,
        cache: str,
        filter: CacheWatchFilter,
        *,
        start: int | None = None,
        retained: bool = False,
    ) -> CacheWatchHandle: ...
    def stream_shards(self, tenant_id: str, namespace: str, stream: str) -> int: ...
    def subscribe_sharded(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        *,
        start: StartPosition | None = None,
        resume: dict[int, int] | None = None,
    ) -> ShardedSubscriptionHandle: ...
    def endpoints(self) -> list[str]: ...
    def offset_for_time(
        self, tenant_id: str, namespace: str, stream: str, shard: int, at_micros: int
    ) -> int | None: ...
    def read(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        from_offset: int,
        *,
        end: int | None = None,
        max_records: int = 0,
    ) -> StreamPage: ...
    def publish_batch(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        payloads: list[bytes],
        *,
        key: bytes | None = None,
        ack: AckMode = "per_message",
    ) -> int | None: ...
    def publish_records(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        records: list[tuple[bytes, bytes]],
        *,
        ack: AckMode = "per_message",
        at_least_once: bool = False,
    ) -> BatchReport: ...
    def refresh_topology(self) -> int: ...
    def connections_per_node(self) -> list[tuple[str, int]]: ...
    def group_dead_letter(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str, offset: int
    ) -> None: ...
    def group_nack_after(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        group: str,
        record: GroupRecord,
        delay: float,
    ) -> None: ...
    def group_seek_stream(
        self, tenant_id: str, namespace: str, stream: str, group: str, start: StartPosition
    ) -> list[GroupPosition]: ...
    def group_create_stream(
        self, tenant_id: str, namespace: str, stream: str, group: str, start: StartPosition
    ) -> list[GroupPosition]: ...
    def group_describe_stream(
        self, tenant_id: str, namespace: str, stream: str, group: str
    ) -> list[GroupInfo]: ...
    def group_delete_stream(
        self, tenant_id: str, namespace: str, stream: str, group: str
    ) -> bool: ...
    def group_sharded(
        self, tenant_id: str, namespace: str, stream: str, group: str
    ) -> ShardedGroupHandle: ...
    def watch_cache_sharded(
        self,
        tenant_id: str,
        namespace: str,
        cache: str,
        prefix: str,
        *,
        resume: dict[int, int] | None = None,
        retained: bool = False,
    ) -> ShardedCacheWatchHandle: ...
    def __enter__(self) -> "Client": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...

class AsyncSubscription:
    """A subscription consumed with ``async for``."""

    start_offset: int | None
    live_offset: int | None

    @property
    def closed(self) -> Awaitable[bool]: ...
    async def next_event(self, timeout: float | None = None) -> Event | None: ...
    async def close(self) -> None: ...
    def __aiter__(self) -> AsyncIterator[Event]: ...
    async def __anext__(self) -> Event: ...
    async def __aenter__(self) -> "AsyncSubscription": ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...

class AsyncClient:
    """The asyncio surface. Construct with ``await AsyncClient.connect(...)``."""

    @staticmethod
    async def connect(
        addrs: str | Sequence[str],
        *,
        tenant_id: str,
        token: str | None = None,
        token_provider: Callable[[], Awaitable[str]] | None = None,
        server_name: str = "localhost",
        ca_file: str | None = None,
        offer_alpn: bool = False,
        reconnect_attempts: int | None = None,
        reconnect_deadline: float | None = None,
    ) -> "AsyncClient":
        """``token_provider`` must be an ``async def`` (or return an awaitable); one returning a ``str`` fails with ``AuthError``.

        It is awaited on the loop running this call, which must stay open for the life of the client and its ``with_identity`` handles.

        The provider's task is cancelled (it sees ``asyncio.CancelledError``) when a call passes the 30 s fetch timeout and when the client, its handles, its open subscriptions and its in-flight calls are all gone."""
    async def with_identity(
        self,
        tenant_id: str,
        token: str | None = None,
        *,
        token_provider: Callable[[], Awaitable[str]] | None = None,
    ) -> "AsyncClient": ...
    async def publish(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        payload: bytes,
        *,
        key: bytes | None = None,
        ack: AckMode = "per_message",
        at_least_once: bool = False,
    ) -> int | None: ...
    async def publish_on_shard(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        payloads: list[bytes],
        *,
        ack: AckMode = "per_message",
    ) -> int | None: ...
    async def producer(self) -> AsyncProducer: ...
    async def subscribe(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        *,
        start: StartPosition | None = None,
        shard: int = 0,
    ) -> AsyncSubscription: ...
    async def cache_put(
        self,
        tenant_id: str,
        namespace: str,
        cache: str,
        key: str,
        value: bytes,
        *,
        ttl: float | None = None,
    ) -> None: ...
    async def cache_get(
        self, tenant_id: str, namespace: str, cache: str, key: str
    ) -> bytes | None: ...
    async def cache_delete(
        self, tenant_id: str, namespace: str, cache: str, key: str
    ) -> bytes | None: ...
    async def commit(
        self, tenant_id: str, namespace: str, entity_key: bytes, ops: list[CommitOp]
    ) -> CommitReceipt: ...
    async def state_get(
        self, tenant_id: str, namespace: str, stream: str, entity_key: bytes, key: str
    ) -> StateValue: ...
    async def cache_get_versioned(
        self, tenant_id: str, namespace: str, cache: str, key: str
    ) -> VersionedValue | None: ...
    async def cache_put_if(
        self,
        tenant_id: str,
        namespace: str,
        cache: str,
        key: str,
        value: bytes,
        condition: CacheCondition,
        *,
        ttl: float | None = None,
    ) -> CacheConditionResult: ...
    async def cache_delete_if(
        self, tenant_id: str, namespace: str, cache: str, key: str, version: int
    ) -> CacheConditionResult: ...
    async def commit_if(
        self,
        tenant_id: str,
        namespace: str,
        entity_key: bytes,
        ops: list[CommitOp],
        expected_offset: int,
    ) -> ConditionalWrite: ...
    async def commit_on_shard(
        self,
        tenant_id: str,
        namespace: str,
        shard: int,
        entity_key: bytes,
        ops: list[CommitOp],
    ) -> CommitReceipt: ...
    async def commit_if_on_shard(
        self,
        tenant_id: str,
        namespace: str,
        shard: int,
        entity_key: bytes,
        ops: list[CommitOp],
        expected_offset: int,
    ) -> ConditionalWrite: ...
    async def state_get_on_shard(
        self, tenant_id: str, namespace: str, stream: str, shard: int, key: str
    ) -> StateValue: ...
    async def publish_if(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        payloads: list[bytes],
        expected_offset: int,
        *,
        key: bytes | None = None,
    ) -> ConditionalWrite: ...
    async def publish_if_on_shard(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        payloads: list[bytes],
        expected_offset: int,
    ) -> ConditionalWrite: ...
    async def counter_add(
        self, tenant_id: str, namespace: str, cache: str, key: str, delta: int
    ) -> int: ...
    async def counter_get(
        self, tenant_id: str, namespace: str, cache: str, key: str
    ) -> int | None: ...
    async def group_poll(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        group: str,
        *,
        max_records: int = 32,
        wait: float = 5.0,
        consumer: str | None = None,
        reclaim: bool = False,
        visibility: float | None = None,
    ) -> list[GroupRecord]: ...
    async def group_ack(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str, offset: int
    ) -> None: ...
    async def group_nack(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str, record: GroupRecord
    ) -> None: ...
    async def group_dead_letters(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str
    ) -> list[int]: ...
    async def group_discard(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str, offset: int
    ) -> None: ...
    async def group_redrive(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str, offset: int
    ) -> None: ...
    async def group_extend(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        group: str,
        record: GroupRecord,
        extend: float,
    ) -> float: ...
    async def group_seek(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        group: str,
        start: StartPosition,
    ) -> GroupPosition: ...
    async def group_create(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        group: str,
        start: StartPosition,
    ) -> GroupPosition: ...
    async def group_describe(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str
    ) -> GroupInfo: ...
    async def group_delete(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str
    ) -> bool: ...
    async def watch_cache(
        self,
        tenant_id: str,
        namespace: str,
        cache: str,
        filter: CacheWatchFilter,
        *,
        start: int | None = None,
        retained: bool = False,
    ) -> AsyncCacheWatch: ...
    async def stream_shards(self, tenant_id: str, namespace: str, stream: str) -> int: ...
    async def subscribe_sharded(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        *,
        start: StartPosition | None = None,
        resume: dict[int, int] | None = None,
    ) -> AsyncShardedSubscription: ...
    async def endpoints(self) -> list[str]: ...
    async def offset_for_time(
        self, tenant_id: str, namespace: str, stream: str, shard: int, at_micros: int
    ) -> int | None: ...
    async def read(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        from_offset: int,
        *,
        end: int | None = None,
        max_records: int = 0,
    ) -> StreamPage: ...
    async def publish_batch(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        payloads: list[bytes],
        *,
        key: bytes | None = None,
        ack: AckMode = "per_message",
    ) -> int | None: ...
    async def publish_records(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        records: list[tuple[bytes, bytes]],
        *,
        ack: AckMode = "per_message",
        at_least_once: bool = False,
    ) -> BatchReport: ...
    async def refresh_topology(self) -> int: ...
    async def connections_per_node(self) -> list[tuple[str, int]]: ...
    async def group_dead_letter(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str, offset: int
    ) -> None: ...
    async def group_nack_after(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        shard: int,
        group: str,
        record: GroupRecord,
        delay: float,
    ) -> None: ...
    async def group_seek_stream(
        self, tenant_id: str, namespace: str, stream: str, group: str, start: StartPosition
    ) -> list[GroupPosition]: ...
    async def group_create_stream(
        self, tenant_id: str, namespace: str, stream: str, group: str, start: StartPosition
    ) -> list[GroupPosition]: ...
    async def group_describe_stream(
        self, tenant_id: str, namespace: str, stream: str, group: str
    ) -> list[GroupInfo]: ...
    async def group_delete_stream(
        self, tenant_id: str, namespace: str, stream: str, group: str
    ) -> bool: ...
    async def group_sharded(
        self, tenant_id: str, namespace: str, stream: str, group: str
    ) -> AsyncShardedGroup: ...
    async def watch_cache_sharded(
        self,
        tenant_id: str,
        namespace: str,
        cache: str,
        prefix: str,
        *,
        resume: dict[int, int] | None = None,
        retained: bool = False,
    ) -> AsyncShardedCacheWatch: ...
    async def __aenter__(self) -> "AsyncClient": ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...
