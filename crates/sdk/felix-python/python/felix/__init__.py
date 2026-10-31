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
  that kept failing. ``group_extend`` keeps a claim standing, and
  ``group_create``, ``group_seek``, ``group_describe`` and ``group_delete``
  administer a group's position, and the ``*_stream`` forms do the same on
  every shard. ``group_nack_after`` delays a redelivery, ``group_dead_letter``
  gives a record up, ``group_poll(visibility=...)`` sets how long a claim
  stands, and ``group_sharded`` polls every shard through one handle.
* **Cache watches** — ``watch_cache`` with a key or prefix filter, resumable
  by offset, or ``retained=True`` to receive current state before live changes.
  ``watch_cache_sharded`` merges a prefix across every shard of a cache.
* **Multi-shard streams** — ``subscribe_sharded`` opens one subscription per
  shard, follows each shard's own owner, and merges them. ``subscribe(shard=)``
  reads one named shard.
* **Reading history** — ``offset_for_time`` finds where a time begins, and
  ``read`` returns a page of committed records without subscribing.
* **Batches** — ``publish_batch`` sends one ordered batch, and
  ``publish_records`` splits keyed records by shard and reports each shard's
  outcome in a ``BatchReport``.
* **Atomic commits** — ``commit`` writes an event and state updates as one
  record on the shard an entity key routes to, and ``state_get`` reads that
  state with the commit's offset as its version. See docs/atomic-commit.md.
* **Conditional writes** — ``cache_put_if``, ``cache_delete_if`` and
  ``cache_get_versioned`` write a cache key only at the version just read.
  ``publish_if`` and ``commit_if`` write only if the shard's next offset is the
  one expected, and ``commit_on_shard`` names the shard. A refusal is a result,
  not an error, and a lost answer is never resent. ``trim_before`` hides a
  durable shard's oldest records the same way, only if the tail is as expected.
* **Idempotent producers** — ``producer()`` returns a producer whose batches are
  numbered, so one that got no answer can be sent again and lands once.
  ``publish_on_shard`` sends a batch to a shard you name.
"""

from ._felix import (
    AsyncShardedCacheWatch,
    AsyncShardedGroup,
    BatchReport,
    CacheShardClosed,
    CacheShardLagged,
    CacheStateComplete,
    ShardPublish,
    ShardedCacheChange,
    ShardedCacheWatchHandle,
    ShardedGroupHandle,
    ShardedGroupRecord,
    StreamPage,
    StreamRecord,
    AsyncCacheWatch,
    AsyncClient,
    AsyncProducer,
    AsyncShardedSubscription,
    AsyncSubscription,
    AuthError,
    CacheChange,
    CacheCondition,
    CacheConditionResult,
    CacheWatchFilter,
    CacheWatchHandle,
    CacheWatchLagged,
    CacheWatchShardMoved,
    Client,
    CommitError,
    CommitOp,
    CommitReceipt,
    ConditionalTrim,
    ConditionalWrite,
    ConnectionError,
    CursorError,
    Event,
    EventCountError,
    FelixError,
    GroupInfo,
    GroupPosition,
    GroupRecord,
    NotFoundError,
    NotOnOwningShardError,
    OutcomeUnknownError,
    OverloadedError,
    Producer,
    PublishRefusedError,
    ShardedSubscriptionHandle,
    ShardLost,
    ShardMoved,
    ShardRecord,
    ShardRecovered,
    ShardUnavailableError,
    StateValue,
    StreamDeleted,
    SubscriptionHandle,
    TrimError,
    VersionedValue,
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
    "StreamPage",
    "StreamRecord",
    "BatchReport",
    "ShardPublish",
    # Multi-shard streams
    "ShardedSubscriptionHandle",
    "AsyncShardedSubscription",
    "ShardRecord",
    "ShardLost",
    "ShardRecovered",
    "ShardMoved",
    "StreamDeleted",
    # Queues
    "GroupRecord",
    "GroupPosition",
    "GroupInfo",
    "ShardedGroupHandle",
    "AsyncShardedGroup",
    "ShardedGroupRecord",
    # Cache watches
    "CacheWatchFilter",
    "CacheWatchHandle",
    "AsyncCacheWatch",
    "CacheChange",
    "CacheWatchLagged",
    "CacheWatchShardMoved",
    "ShardedCacheWatchHandle",
    "AsyncShardedCacheWatch",
    "ShardedCacheChange",
    "CacheStateComplete",
    "CacheShardLagged",
    "CacheShardClosed",
    # Atomic commits
    "CommitOp",
    "CommitReceipt",
    "StateValue",
    # Conditional writes
    "CacheCondition",
    "CacheConditionResult",
    "VersionedValue",
    "ConditionalTrim",
    "ConditionalWrite",
    # Idempotent producers
    "Producer",
    "AsyncProducer",
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
    "PublishRefusedError",
    "TrimError",
    "__version__",
]
