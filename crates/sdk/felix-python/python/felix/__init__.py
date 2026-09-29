"""Felix client for Python.

A thin wrapper over the Rust client (`felix-client`), which is where
reconnection, redirect-following, retry classification and offset bookkeeping
actually live. Nothing about the protocol is reimplemented here, so Python
gets the same failover behaviour Rust does rather than its own approximation
of it.

    import felix

    with felix.Client("127.0.0.1:5000", tenant_id="t1", token=tok) as client:
        client.publish("t1", "default", "events", b"hello")

        with client.subscribe("t1", "default", "events") as events:
            for event in events:
                print(event.payload, event.offset)

For asyncio — which is what most Python realtime backends are — use
``AsyncClient``, whose methods are awaitable and whose subscriptions are
``async for``:

    async with await felix.AsyncClient.connect(addr, tenant_id=..., token=...) as client:
        await client.publish("t1", "default", "events", b"hello")
        async with await client.subscribe("t1", "default", "events") as events:
            async for event in events:
                print(event.payload, event.offset)

Both surfaces wrap the same Rust client, so they fail over identically. The
synchronous one blocks with the GIL released, which suits threads and
``asyncio.to_thread``; the async one yields to your event loop.

Beyond streams, both surfaces cover:

* **Queues** — ``group_poll`` / ``group_ack`` / ``group_nack``, with
  ``group_dead_letters``, ``group_redrive`` and ``group_discard`` for records
  that kept failing.
* **Cache watches** — ``watch_cache`` with a key or prefix filter, resumable
  by offset, or ``retained=True`` to receive current state before live changes.
* **Multi-shard streams** — ``subscribe_sharded`` opens one subscription per
  shard, follows each shard's own owner, and merges them.
* **Atomic commits** — ``commit`` writes an event and state updates as one
  record on the shard an entity key routes to, and ``state_get`` reads that
  state with the commit's offset as its version. See docs/atomic-commit.md.
"""

from ._felix import (
    AsyncCacheWatch,
    AsyncClient,
    AsyncShardedSubscription,
    AsyncSubscription,
    AuthError,
    CacheChange,
    CacheWatchFilter,
    CacheWatchHandle,
    CacheWatchLagged,
    CacheWatchShardMoved,
    Client,
    CommitError,
    CommitOp,
    CommitReceipt,
    ConnectionError,
    CursorError,
    Event,
    EventCountError,
    FelixError,
    GroupRecord,
    NotFoundError,
    NotOnOwningShardError,
    OutcomeUnknownError,
    OverloadedError,
    ShardedSubscriptionHandle,
    ShardLost,
    ShardMoved,
    ShardRecord,
    ShardRecovered,
    ShardUnavailableError,
    StateValue,
    SubscriptionHandle,
    __version__,
)

__all__ = [
    # Clients
    "Client",
    "AsyncClient",
    # Streams
    "Event",
    "SubscriptionHandle",
    "AsyncSubscription",
    # Multi-shard streams
    "ShardedSubscriptionHandle",
    "AsyncShardedSubscription",
    "ShardRecord",
    "ShardLost",
    "ShardRecovered",
    "ShardMoved",
    # Queues
    "GroupRecord",
    # Cache watches
    "CacheWatchFilter",
    "CacheWatchHandle",
    "AsyncCacheWatch",
    "CacheChange",
    "CacheWatchLagged",
    "CacheWatchShardMoved",
    # Atomic commits
    "CommitOp",
    "CommitReceipt",
    "StateValue",
    # Errors
    "FelixError",
    "ConnectionError",
    "AuthError",
    "NotFoundError",
    "CursorError",
    "ShardUnavailableError",
    "OverloadedError",
    "OutcomeUnknownError",
    "CommitError",
    "NotOnOwningShardError",
    "EventCountError",
    "__version__",
]
