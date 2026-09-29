---
title: "Python Client"
description: "Installing and using the Felix Python client: both surfaces, streams, queues, cache watches, multi-shard consumption, and the failure modes worth writing code for."
---

`felix` is a binding over the Rust client. Reconnection,
redirect-following, retry classification and offset bookkeeping live in
`felix-client` and are shared, so Python gets the same failover behaviour as
Rust. See
[Choosing a Client](/felix/clients/overview/) for why that choice was made and
what the conformance suite does about it.

## Installing

```bash
pip install felix-client
```

Wheels ship compiled, one per platform, for every Python from 3.9 up, so
installing needs no Rust toolchain. Linux (x86-64 and arm64), macOS (Intel and
Apple silicon) and Windows x86-64 are covered; anything else builds from the
sdist and does need one.

Building from the repository needs a Rust toolchain either way:

```bash
pip install ./crates/sdk/felix-python
```

## Two surfaces

Both wrap the same Rust client and fail over identically. Pick by how your
application is already written; performance is the same.

| | `Client` | `AsyncClient` |
| --- | --- | --- |
| Calls | block, GIL released | awaitable |
| Subscriptions | `for event in events` | `async for event in events` |
| Suits | threads, `asyncio.to_thread`, scripts | an existing event loop |

```python
import felix

with felix.Client("127.0.0.1:5000", tenant_id="t1", token=tok) as client:
    client.publish("t1", "default", "events", b"hello")
```

```python
client = await felix.AsyncClient.connect(
    "127.0.0.1:5000", tenant_id="t1", token=tok
)
async with client:
    await client.publish("t1", "default", "events", b"hello")
```

The synchronous surface releases the GIL while it blocks, so threads run in
parallel. A `concurrent.futures.ThreadPoolExecutor` over one shared client is a
reasonable design, not a queue behind a lock.

## Connecting

```python
felix.Client(
    addrs,                    # "host:port", or a list of them
    tenant_id="t1",
    token=tok,
    server_name="localhost",  # the name the broker's certificate carries
    ca_file=None,             # trust a specific CA; omit for the system store
    offer_alpn=False,         # offer the felix/1 ALPN
)
```

One reachable address is enough. The client discovers the rest of the cluster
and will use brokers it was never told about. Passing several only helps the
*first* connection, for the case where the one you named is the one that is
down.

TLS is always on. QUIC has no unencrypted mode, so there are two trust
choices: the platform trust store, or an explicit `ca_file` for a self-signed
development broker. There is no "skip verification" switch. It would silently
turn a secure deployment insecure, and a CA file covers development without it.

`offer_alpn=True` makes the client offer the `felix/1` ALPN, which a broker
running with `FELIX_TLS_REQUIRE_ALPN=true` insists on. It is off by default
because a broker older than ALPN support refuses a client that offers it.

## Publishing

```python
client.publish("t1", "default", "orders", payload)                      # acked
client.publish("t1", "default", "orders", payload, ack="none")          # fire and forget
client.publish("t1", "default", "orders", payload, key=customer_id)     # routed
```

`ack` selects what the broker must have done before the call returns:
`"per_message"` (the default), `"per_batch"`, or `"none"`, which promises
nothing and does not wait to find out.

### The routing key decides the shard

**Without a key every record lands on shard 0**, so a multi-shard stream
behaves like a single-shard one. If you created a stream with several shards to
get throughput, and you are not passing a key, you are not getting it.

```python
for order in orders:
    client.publish(
        "t1", "default", "orders",
        order.encode(),
        key=order.customer_id.encode(),
    )
```

Records sharing a key share a shard and stay ordered with respect to each
other. Records with different keys do not, once a stream has more than one
shard. A consumer needing total order wants a single-shard stream.

### At-least-once may duplicate

By default a publish whose outcome was ambiguous (the broker may or may not
have written it before the connection went) is **reported, not re-sent**,
because nothing downstream can tell two copies apart.

```python
client.publish("t1", "default", "orders", payload, at_least_once=True)
```

That is the opt-in. The record is then certain to land and **may land twice**.
It cannot be combined with `key`: the re-send path does not carry one, and
honouring the key on the first attempt but not the re-send would put the
duplicate on a different shard, so the combination raises rather than
resolving.

## Subscribing

```python
with client.subscribe("t1", "default", "events") as events:
    for event in events:
        handle(event.payload)
```

`start` is `"latest"` (the default), `"earliest"`, or an integer offset: **the
first record you have not seen**, so a resuming consumer passes the offset it
last handled *plus one*.

`next_event(timeout=...)` returns `None` both when the timeout passes and when
the broker has ended the stream; `closed` is `True` only after the second, so
check it to tell them apart. A lost connection is neither: on a durable stream
the subscription resubscribes from the offset after the last one it delivered,
and where it cannot, it raises a `FelixError`.

**A subscription follows its shard when a rebalance moves it.** The old owner
ends it after delivering what it committed and says where to resume; the client
resubscribes on the new owner and iteration carries on. On a durable stream
nothing is repeated or skipped; an in-memory stream resumes at the new owner's
tail.

### Offsets are how you notice a drop

Subscriber queues shed under the default policy rather than blocking the
publisher, so a subscriber can silently miss records. On a durable stream every
delivered event carries its log offset, and **a jump in them is a drop**. The
exception is a new leader's generation-start record, which takes an offset; the
next event reports it in `skipped_before`:

```python
expected = None
with client.subscribe("t1", "default", "events") as events:
    for event in events:
        start = event.offset - event.skipped_before
        if expected is not None and start != expected:
            log.warning("dropped %d records", start - expected)
        expected = event.offset + 1
        handle(event.payload)
```

That is worth writing even when you do not resume from offsets. It is the only
signal that the queue overflowed.

### A consumer that survives a restart

The pattern the offset semantics exist for: checkpoint what you handled, and
resume at the next one.

```python
def run(client, checkpoint):
    start = checkpoint.load()            # None on a cold start
    start = start + 1 if start is not None else "earliest"

    while True:
        try:
            with client.subscribe("t1", "default", "events", start=start) as events:
                for event in events:
                    handle(event.payload)
                    checkpoint.save(event.offset)
                    start = event.offset + 1
        except felix.ConnectionError:
            # Worth retrying, and the client has already tried other brokers.
            # Resuming from `start` is what makes it lossless.
            time.sleep(1)
        except felix.CursorError:
            # Retention discarded the offset. The only recovery is to accept
            # the gap and say so. Silently restarting at the tail would lose
            # records without telling anyone.
            log.error("checkpoint %s is past retention; restarting at earliest", start)
            start = "earliest"
```

The two `except` clauses do different things. The recovery differs, so the
error types differ.

## Errors you can act on

```python
try:
    client.publish("t1", "default", "orders", payload)
except felix.ShardUnavailableError:
    retry()          # the shard is moving; nothing was written
except felix.OutcomeUnknownError:
    reconcile()      # it may have been written; resend only if idempotent
except felix.ConnectionError:
    retry()          # worth another attempt, against another broker
except felix.AuthError:
    give_up()        # no amount of retrying grants a permission
except felix.NotFoundError:
    create_stream()  # or wait: a broker promoted a moment ago may not know it yet
```

| Exception | What it means | Retry? |
| --- | --- | --- |
| `ConnectionError` | broker unreachable, the connection died mid-call, or the broker is shutting down (`draining`) | yes, elsewhere |
| `ShardUnavailableError` | nobody can serve the shard right now, usually because it is moving (`shard_unavailable`), or another broker owns it (`not_leader`); nothing was written | yes |
| `OverloadedError` | the broker is shedding load (`overloaded`); nothing was written | yes, after a pause |
| `OutcomeUnknownError` | the write may or may not have happened (`quorum_timeout`, `leadership_lost`, `unacknowledged`, or any error sent as `outcome_unknown`) | only if idempotent |
| `AuthError` | token rejected, or missing the permission (`unauthenticated`, `forbidden`) | no |
| `NotFoundError` | no such tenant, namespace, stream or cache (`not_found`) | see `retry` |
| `CursorError` | the start offset is gone; retention discarded it | no; restart at `earliest` |
| `FelixError` | the base, and anything else (`invalid_request`, `limit_exceeded`, a code this client does not know) | see `retry` |

Every exception carries three attributes from the broker:

- `code`: the broker's [error code](https://github.com/gabloe/felix/blob/main/docs/protocol.md#error-codes),
  such as `"shard_unavailable"` or `"quorum_timeout"`.
- `retry`: what you may do about it: `"retry"`, `"retry_after"`,
  `"redirect"`, `"outcome_unknown"` or `"fatal"`.
- `detail`: a dict of extra facts, such as `{"reason": "fenced"}` for an
  unavailable shard or `retry_after_ms`, or `None`.

The class is picked from `code`, except that an `outcome_unknown` retry class
always makes an `OutcomeUnknownError`: whatever went wrong, "this may have been
written" decides what you do next. When the two disagree, act on `retry`. A
code this client does not know still carries a retry class.

All three are `None` when the broker predates error codes, or when the failure
happened in the client (a lost connection, say). Then the class is chosen from
the message.

Never match on the message. It is prose and it will be reworded; match on the
exception type or `code` instead.

## Queues

A queue works differently from a subscription. Records are **pulled**, because
only the consumer knows when it has capacity; each is claimed by one member
until settled; and an unsettled record comes back.

```python
while True:
    records = client.group_poll(
        "t1", "default", "orders", shard, "billing",
        max_records=32,
        wait=5.0,          # long poll; an empty list is an answer, not an error
    )
    for record in records:
        try:
            charge(record.payload)
            client.group_ack("t1", "default", "orders", shard, "billing", record.offset)
        except Retryable:
            # Back to the queue now, rather than after the visibility timeout.
            client.group_nack("t1", "default", "orders", shard, "billing", record.offset)
```

`record.attempts` counts deliveries **including this one**, so `1` is a first
attempt and anything higher is a redelivery. It is how a consumer tells a retry
from a first try, which is worth branching on before doing anything expensive or
side-effecting:

```python
if record.attempts > 3:
    quarantine(record)
    client.group_ack(...)      # settle it; it is not coming back
else:
    process(record)
```

**A group is bound to one shard.** Consuming a multi-shard stream means polling
each shard's group; `stream_shards` says how many there are. Only the shard's leader
serves its group; the client follows the broker's redirect there, including
after a rebalance moves the shard, so any broker address works.

### Dead letters

Past the attempt bound a record is set aside rather than retried forever:

```python
for offset in client.group_dead_letters("t1", "default", "orders", shard, "billing"):
    if fixed_the_cause:
        client.group_redrive("t1", "default", "orders", shard, "billing", offset)
    else:
        client.group_discard("t1", "default", "orders", shard, "billing", offset)
```

## Cache and counters

```python
client.cache_put("t1", "default", "sessions", "user-abc", data, ttl=3600.0)
value = client.cache_get("t1", "default", "sessions", "user-abc")      # bytes or None
removed = client.cache_delete("t1", "default", "sessions", "user-abc") # bytes or None
```

`ttl` is in seconds and takes a float, so `ttl=0.5` is half a second. Leave it
out and the entry has no time-to-live. `cache_get` returns `None` for a key that is
missing or expired. `cache_delete` returns the value it removed, or `None` if
the key was not there, so you can tell a delete that did something from one
that did not.

Counters live beside the cache and use the same scoping:

```python
total = client.counter_add("t1", "default", "limits", "user:42:reqs", 1)  # sum after the add
current = client.counter_get("t1", "default", "limits", "user:42:reqs")   # int or None
```

`counter_add` takes a signed delta and returns the sum including it.
`counter_get` returns `None` for a counter that was never written, which is not
the same as zero. A retry after a lost acknowledgement counts twice. Counters
need a durable broker that advertises `FEATURE_COUNTERS`.

`AsyncClient` has the same methods as coroutines.

## Cache watches

Watches can resume by offset and report loss explicitly, which makes the cache
usable for state synchronisation.

```python
roster = {}
resume_from = None
while True:
    with client.watch_cache(
        "t1", "default", "sessions",
        felix.CacheWatchFilter.prefix("room:42:"),
        start=resume_from,
        retained=resume_from is None,
    ) as watch:
        if resume_from is None:
            # Retained values arrive first, and the count says exactly how many.
            # Zero is a definite answer (the prefix is empty), not a silence to
            # wait through.
            for _ in range(watch.retained_count):
                change = watch.recv()
                roster[change.key] = change.value
        resume_from = None

        # State is now complete. Everything after this is live.
        for item in watch:
            if isinstance(item, felix.CacheWatchLagged):
                # Not an error: the watch did its job by saying so. Offsets on a
                # filtered watch are sparse, so loss cannot be inferred the way a
                # stream subscriber infers it. This is the only signal.
                resume_from = item.resume_from
                break
            if isinstance(item, felix.CacheWatchShardMoved):
                # The shard moved to another broker; the watch follows it there.
                continue
            if item.value is None:
                roster.pop(item.key, None)     # a delete is a change with no value
            else:
                roster[item.key] = item.value
    if resume_from is None:
        break                                  # the broker ended the watch
```

Three things in that example matter:

- **`retained_count`** tells you the moment your state is complete. Without it
  you are guessing when to start trusting the map.
- **`value is None` means removed**, and is deliberately distinguishable from
  an empty value. A watcher mirroring a cache has to tell those apart.
- **`CacheWatchLagged` is a value, not an exception.** Re-watching from
  `resume_from` is gapless. `CacheWatchShardMoved` is only a notice: the watch
  follows its shard to the new owner and carries on, with no change repeated
  or skipped.

A prefix watch reads **one shard**, so a prefix spanning a multi-shard cache
needs one watch per shard. On a multi-shard cache the broker refuses a prefix
watch that names no shard, rather than quietly reading shard 0. The merged
helper, `watch_cache_sharded`, is Rust-only for now.

## Atomic commits

`commit` writes an event and the state it changes as one record on the shard
an entity key routes to. A subscriber, a consumer group and `state_get` all
see the whole commit or none of it, at the same offset.

```python
import felix

stream = "order-events"
receipt = client.commit(
    "acme",
    "orders",
    b"order-42",
    [
        felix.CommitOp.enqueue(stream, b'{"type":"placed","id":42}'),
        felix.CommitOp.put(stream, "order-42", b'{"status":"placed"}'),
        felix.CommitOp.delete(stream, "cart-42"),
    ],
)
print(receipt.offset)  # where the event is read, and the state's version

state = client.state_get("acme", "orders", stream, b"order-42", "order-42")
assert state.version == receipt.offset
print(state.value, state.as_of)
```

`AsyncClient` has the same two methods, awaited.

| Call | Returns |
| --- | --- |
| `commit(tenant_id, namespace, entity_key, ops)` | `CommitReceipt(offset)` |
| `state_get(tenant_id, namespace, stream, entity_key, key)` | `StateValue(value, version, as_of)`; `value` is `None` for a key never written or deleted |
| `CommitOp.publish(stream, payload)`, `CommitOp.enqueue(queue, payload)` | the commit's one event, for subscribers or for consumer groups (the same record: a queue is a stream read through a group) |
| `CommitOp.put(stream, key, value)`, `CommitOp.delete(stream, key)` | state changes, applied in order |

A commit is refused before anything is sent when it cannot be one record:

```python
try:
    client.commit("acme", "orders", b"order-42", [
        felix.CommitOp.publish("order-events", b"placed"),
        felix.CommitOp.put("inventory", "sku-1", b"3"),   # another stream
    ])
except felix.NotOnOwningShardError as err:
    print(err.index, err.stream, err.owner)   # 1 inventory order-events
except felix.EventCountError as err:
    print(err.count)                          # not exactly one event
except felix.CommitError:
    ...                                       # the broker does not support commits
```

Both are subclasses of `CommitError`, itself a `FelixError`. Failures after the
commit was sent are the ordinary classes above; note that a commit is **not
idempotent**, so an `OutcomeUnknownError` means it may already be written.

**What atomic covers:** every part of one commit, on one shard's log, across a
failover. **What it does not:** another stream or shard, a Felix cache (a
commit's state is the stream shard's own, read with `state_get`), or a retry.
State is rebuilt from the retained log, so keep an entity stream's retention
long enough. The stream must be durable, and in a cluster the operator must
finalize the `atomic_commit` fleet feature first. The full semantics are in
[`docs/atomic-commit.md`](https://github.com/gabloe/felix/blob/main/docs/atomic-commit.md).

## Multi-shard streams

A subscription reads one shard. `subscribe_sharded` opens one per shard,
follows each shard's own owner, and merges them:

```python
with client.subscribe_sharded("t1", "default", "orders") as stream:
    for item in stream:
        if isinstance(item, felix.ShardRecord):
            handle(item.event.payload)
        elif isinstance(item, felix.ShardLost):
            # Surfaced rather than swallowed: the other shards carry on, so a
            # consumer that ignored this would be reading part of the stream
            # while believing it read all of it.
            alert(item.shard, item.error)
        elif isinstance(item, felix.ShardRecovered):
            log.info("shard %d back", item.shard)
        elif isinstance(item, felix.ShardMoved):
            # Followed to its new owner; its records carry on from there.
            log.info("shard %d moved to %s", item.shard, item.node_id)
```

Resuming is a **map**, not a number, because offsets are per shard:

```python
positions = stream.positions()     # {shard: last offset handled}
# ... later, after a restart
resumed = client.subscribe_sharded("t1", "default", "orders", resume=positions)
```

A single offset carried across shards would replay on every shard but one.

:::caution[A shard that delivered nothing has no position]
`positions()` only lists shards that handed something over. On resume, a shard
not in the map starts wherever `start` says. That is the live tail by
default, so records published to it while you were away are missed. If that
matters, pass `start="earliest"` alongside `resume`, or make sure every shard
has reported before you checkpoint.
:::

## Concurrency

One client is meant to be shared. The synchronous surface blocks with the GIL
released, so a thread pool over one client runs in parallel:

```python
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    pool.map(lambda p: client.publish("t1", "default", "events", p), payloads)
```

Creating a client per thread is the thing to avoid: each is a QUIC endpoint
with its own connection pool, and they do not share discovery.

## What is not wrapped

- **`at_least_once` with a routing key.** The re-send path does not carry one,
  and the client refuses the combination rather than silently dropping the key.
- **Idempotent producers.** The Rust client's `IdempotentProducer` is not
  wrapped yet.

Both are marked in the conformance catalogue, so the binding reports them as
unclaimed.

## Conformance

Python passes every required scenario in the
[client conformance catalogue](/felix/clients/overview/#the-conformance-suite),
and CI is gated on it. The suite runs against a real three-node cluster rather
than a mock, because what it checks (reconnection, redirect-following, offset
accounting) only exists in a cluster.
