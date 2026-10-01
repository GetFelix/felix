"""Type stubs for the native module.

Kept by hand and checked against the binding by `tests/test_api_surface.py`,
so a signature cannot drift here without a test noticing.
"""

from types import TracebackType
from typing import AsyncIterator, Awaitable, Iterator, Literal, Sequence

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

class Event:
    tenant_id: str
    namespace: str
    stream: str
    payload: bytes
    offset: int | None
    skipped_before: int

class SubscriptionHandle:
    closed: bool
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

ShardEvent = ShardRecord | ShardLost | ShardRecovered | ShardMoved

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

class Client:
    def __init__(
        self,
        addrs: str | Sequence[str],
        *,
        tenant_id: str,
        token: str,
        server_name: str = "localhost",
        ca_file: str | None = None,
        offer_alpn: bool = False,
    ) -> None: ...
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
    def subscribe(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        *,
        start: StartPosition | None = None,
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
    ) -> list[GroupRecord]: ...
    def group_ack(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str, offset: int
    ) -> None: ...
    def group_nack(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str, offset: int
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
    def __enter__(self) -> "Client": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...

class AsyncSubscription:
    """A subscription consumed with ``async for``."""

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
        token: str,
        server_name: str = "localhost",
        ca_file: str | None = None,
        offer_alpn: bool = False,
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
    async def subscribe(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        *,
        start: StartPosition | None = None,
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
    ) -> list[GroupRecord]: ...
    async def group_ack(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str, offset: int
    ) -> None: ...
    async def group_nack(
        self, tenant_id: str, namespace: str, stream: str, shard: int, group: str, offset: int
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
    async def __aenter__(self) -> "AsyncClient": ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...
