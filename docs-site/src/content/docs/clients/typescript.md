---
title: "TypeScript Client"
description: "Installing and using the Felix Node.js client: promises, typed errors, disposal, streams, queues, cache watches, multi-shard consumption, and the failure modes worth writing code for."
---

`felix-client` on npm is a napi-rs addon over the Rust crate of the same name,
not a reimplementation. Reconnection, redirect-following, retry classification
and offset bookkeeping live in the crate and are shared; Node gets the same
failover behaviour Rust does rather than its own approximation. The name is
deliberately identical on crates.io, PyPI and npm. See
[Choosing a Client](/felix/clients/overview/) for why that choice was made.

## Installing

```bash
npm install felix-client
```

The binary ships as one package per platform, declared as optional
dependencies, so npm fetches only the one your machine needs — nothing is
compiled at install time and no Rust toolchain is required. Linux (x86-64 and
arm64, glibc), macOS (Intel and Apple silicon) and Windows x86-64 are covered.

To build it from the repository instead:

```bash
cd crates/sdk/felix-typescript
napi build --platform --release      # needs: npm i -g @napi-rs/cli@2
```

Without the napi CLI, `napi build` is mostly a rename — a plain `cargo build`
produces a loadable addon and the package finds it:

```bash
cargo build --release
```

Requires Node 18 or newer.

## One surface, and it is asynchronous

Python offers two surfaces because its synchronous one is the older idiom. Node
has no such split: blocking the event loop is not something a library may do,
so **every call returns a `Promise`**. napi-rs runs the future on its own Tokio
runtime and settles the promise from there, which keeps the event loop free
while a publish is in flight.

```ts
import { Client } from "felix-client";

const client = await Client.connect("127.0.0.1:5000", "t1", token, "localhost", caFile);
await client.publish("t1", "default", "events", Buffer.from("hello"));
client.close();
```

## Connecting

```ts
Client.connect(
  addrs,        // "host:port", or an array of them
  tenantId,
  token,
  serverName,   // the name the broker's certificate carries; defaults to "localhost"
  caFile,       // trust a specific CA; omit for the system trust store
  offerAlpn,    // offer the felix/1 ALPN; defaults to false
)
```

One reachable address is enough — the client discovers the rest of the cluster
and will use brokers it was never told about. Passing several only helps the
*first* connection.

**TLS is not optional.** QUIC has no unencrypted mode, so there are two trust
choices and no third: the platform trust store, or an explicit `caFile` for a
self-signed development broker. There is deliberately no "skip verification"
switch.

`offerAlpn: true` makes the client offer the `felix/1` ALPN, which a broker
running with `FELIX_TLS_REQUIRE_ALPN=true` insists on. It is off by default
because a broker older than ALPN support refuses a client that offers it.

## Disposal

Every handle has an idempotent `close()` and implements `Symbol.asyncDispose`,
so on Node 24 and newer a `throw` releases it on the way out:

```ts
await using events = await client.subscribe("t1", "default", "events");
```

The package itself asks only for Node 18 — `await using` is the syntax that
needs the newer runtime, not the disposal. On older Node, call `close()` in a
`finally`.

**`close()` cancels a read in flight rather than waiting for it.** A consumer
shutting down is almost always parked on `nextEvent`, and waiting for the read
it is cancelling would hang exactly the path that needs to make progress.

## Publishing

```ts
await client.publish("t1", "default", "orders", payload);                          // acked
await client.publish("t1", "default", "orders", payload, undefined, "none");       // fire and forget
await client.publish("t1", "default", "orders", payload, Buffer.from(customerId)); // routed
```

Arguments are positional: `(tenantId, namespace, stream, payload, key?, ack?,
atLeastOnce?)`. Pass `undefined` to skip one.

### The routing key decides the shard

**Without a key every record lands on shard 0**, so a multi-shard stream
behaves like a single-shard one. If you created a stream with several shards to
get throughput and are not passing a key, you are not getting it.

```ts
for (const order of orders) {
  await client.publish(
    "t1", "default", "orders",
    Buffer.from(JSON.stringify(order)),
    Buffer.from(order.customerId),
  );
}
```

Records sharing a key share a shard and stay ordered with respect to each
other; records with different keys do not, once a stream has more than one
shard.

### At-least-once duplicates, and says so

By default a publish whose outcome was ambiguous is **reported, not re-sent** —
nothing downstream can tell two copies apart.

```ts
await client.publish("t1", "default", "orders", payload, undefined, "per_message", true);
```

The record is then certain to land and **may land twice**. It cannot be
combined with a key: the re-send path does not carry one, so the combination is
refused rather than silently resolved.

## Subscribing

```ts
const events = await client.subscribe("t1", "default", "events");
try {
  for (;;) {
    const event = await events.nextEvent();
    if (event === null) break;          // the subscription ended
    handle(event.payload);
  }
} finally {
  await events.close();
}
```

`start` is `"latest"` (the default), `"earliest"`, or a `bigint` offset — **the
first record you have not seen**, so a resuming consumer passes the offset it
last handled *plus one*. Offsets are `bigint`, so the arithmetic is `+ 1n`.

**A subscription follows its shard when a rebalance moves it.** The old owner
ends it after delivering what it committed and says where to resume; the client
resubscribes on the new owner and `nextEvent` carries on. On a durable stream
nothing is repeated or skipped; an in-memory stream resumes at the new owner's
tail.

:::caution[Do not abandon a `nextEvent` you raced against a timer]
There is no timeout argument, because a caller who wants one can race the
promise. But the losing `nextEvent` **stays in flight and will resolve with the
next record** — so keep the promise and await it again rather than calling
`nextEvent` afresh, or you will drop the record it was about to hand you.

```ts
let pending = null;
async function next(timeoutMs) {
  pending ??= events.nextEvent();
  const item = await Promise.race([pending, timer(timeoutMs)]);
  if (item === TIMED_OUT) return null;   // pending is kept for next time
  pending = null;
  return item;
}
```
:::

### Offsets are how you notice a drop

Subscriber queues shed under the default policy rather than blocking the
publisher, so a subscriber can silently miss records. On a durable stream every
event carries its log offset, and **a jump in them is a drop** — except where a
new leader's generation-start record took an offset, which the next event
reports in `skippedBefore`:

```ts
let expected = null;
for (;;) {
  const event = await events.nextEvent();
  if (event === null) break;
  const from = event.offset - event.skippedBefore;
  if (expected !== null && from !== expected) {
    console.warn(`dropped ${from - expected} records`);
  }
  expected = event.offset + 1n;
  handle(event.payload);
}
```

### A consumer that survives a restart

```ts
async function run(client, checkpoint) {
  let start = await checkpoint.load();           // null on a cold start
  start = start === null ? "earliest" : start + 1n;

  for (;;) {
    const events = await client.subscribe("t1", "default", "events", start);
    try {
      for (;;) {
        const event = await events.nextEvent();
        if (event === null) break;
        await handle(event.payload);
        await checkpoint.save(event.offset);
        start = event.offset + 1n;
      }
    } catch (err) {
      if (err instanceof ConnectionError) {
        await sleep(1000);                       // retryable; resume from `start`
      } else if (err instanceof CursorError) {
        // Retention discarded the offset. Accept the gap and say so — silently
        // restarting at the tail would lose records without telling anyone.
        console.error(`checkpoint ${start} is past retention; restarting at earliest`);
        start = "earliest";
      } else {
        throw err;
      }
    } finally {
      await events.close();
    }
  }
}
```

## Errors you can act on

```ts
import {
  AuthError,
  ConnectionError,
  NotFoundError,
  OutcomeUnknownError,
  ShardUnavailableError,
} from "felix-client";

try {
  await client.publish("t1", "default", "orders", payload);
} catch (err) {
  if (err instanceof OutcomeUnknownError) reconcile();  // it may have been written
  else if (err.retryable) retry();                      // nothing was written
  else if (err instanceof AuthError) giveUp();          // retrying grants no permission
  else if (err instanceof NotFoundError) createStream();
  else throw err;
}
```

| Class | What it means | `retryable` without a broker code |
| --- | --- | --- |
| `ConnectionError` | broker unreachable, the connection died mid-call, or the broker is shutting down (`draining`) | `true` |
| `ShardUnavailableError` | nobody can serve the shard right now, usually because it is moving (`shard_unavailable`), or another broker owns it (`not_leader`); nothing was written | `true` |
| `OverloadedError` | the broker is shedding load (`overloaded`); nothing was written | `true` |
| `OutcomeUnknownError` | the write may or may not have happened (`quorum_timeout`, `leadership_lost`, `unacknowledged`, or any error sent as `outcome_unknown`) | `false` |
| `AuthError` | token rejected, or missing the permission (`unauthenticated`, `forbidden`) | `false` |
| `NotFoundError` | no such tenant, namespace, stream or cache (`not_found`) | `false` |
| `CursorError` | the start offset is gone; retention discarded it | `false` |
| `InvalidArgumentError` | a bad argument to this client | `false` |
| `FelixError` | the base, and anything else (`invalid_request`, `limit_exceeded`, a code this client does not know) | `false` |

Every error carries what the broker said about it:

- `code`: the broker's [error code](https://github.com/gabloe/felix/blob/main/docs/protocol.md#error-codes),
  such as `"shard_unavailable"` or `"quorum_timeout"`.
- `retry`: what you may do about it: `"retry"`, `"retry_after"`,
  `"redirect"`, `"outcome_unknown"` or `"fatal"`.
- `detail`: extra facts, such as `{ reason: "fenced" }` for an unavailable
  shard or `retry_after_ms`.

`retryable` follows `retry` when the broker sent one: `true` for `retry`,
`retry_after` and `redirect`, `false` otherwise. So a `NotFoundError` can be
retryable. The broker sends `not_found` as `retry_after`, because a broker
promoted a moment ago may not know the stream yet. The class is picked from the
code, except that an `outcome_unknown` retry class always makes an
`OutcomeUnknownError`.

All three are `undefined` when the broker predates error codes, or when the failure
happened in the client. Then the class is chosen from the message, the way
older versions of this client always did.

`err.kind` is this client's own name for the class (`FELIX_CONNECTION`,
`FELIX_SHARD_UNAVAILABLE`, …), set whether or not the broker sent a code, for
code that would rather switch than test `instanceof`. Never match on the message — it is prose and will be reworded.

## Queues

Records are **pulled**, because only the consumer knows when it has capacity;
each is claimed by one member until settled; an unsettled record comes back.

```ts
for (;;) {
  const records = await client.groupPoll(
    "t1", "default", "orders", shard, "billing",
    32,        // maxRecords
    5000,      // waitMs — a long poll; an empty array is an answer, not an error
  );

  for (const record of records) {
    try {
      await charge(record.payload);
      await client.groupAck("t1", "default", "orders", shard, "billing", record.offset);
    } catch {
      // Back to the queue now, rather than after the visibility timeout.
      await client.groupNack("t1", "default", "orders", shard, "billing", record.offset);
    }
  }
}
```

`record.attempts` counts deliveries **including this one**, so `1` is a first
attempt and anything higher is a redelivery — worth branching on before doing
anything expensive or side-effecting:

```ts
if (record.attempts > 3) {
  await quarantine(record);
  await client.groupAck(...);     // settle it; it is not coming back
}
```

**A group is bound to one shard.** Consuming a multi-shard stream means polling
each shard's group — `streamShards` says how many there are. Only the shard's leader
serves its group; the client follows the broker's redirect there, including
after a rebalance moves the shard, so any broker address works.

### Dead letters

```ts
const offsets = await client.groupDeadLetters("t1", "default", "orders", shard, "billing");
for (const offset of offsets) {
  if (fixedTheCause) {
    await client.groupRedrive("t1", "default", "orders", shard, "billing", offset);
  } else {
    await client.groupDiscard("t1", "default", "orders", shard, "billing", offset);
  }
}
```

## Cache watches

What makes the cache a state-synchronisation primitive rather than a
notification: resume by offset, and loss that is loud.

```ts
const watch = await client.watchCache(
  "t1", "default", "sessions",
  undefined,        // key — or a prefix, below
  "room:42:",       // prefix
  undefined,        // start offset
  true,             // retained
);

const roster = new Map();
try {
  // Retained values arrive first, and the count says exactly how many. 0n is a
  // definite answer — the prefix is empty — not a silence to wait through.
  for (let i = 0n; i < watch.retainedCount; i++) {
    const { change } = await watch.recv();
    roster.set(change.key, change.value);
  }

  // State is now complete. Everything after this is live.
  for (;;) {
    const item = await watch.recv();
    if (item === null) break;
    if (item.laggedResumeFrom !== null) {
      // Not an error: the watch did its job by saying so. Offsets on a filtered
      // watch are sparse, so loss cannot be inferred the way a stream
      // subscriber infers it — this is the only signal.
      return resumeFrom(item.laggedResumeFrom);
    }
    if (item.shardMoved !== null) {
      // The shard moved to another broker; the watch follows it there.
      continue;
    }
    if (item.change.value === null) {
      roster.delete(item.change.key);   // a delete is a change with no value
    } else {
      roster.set(item.change.key, item.change.value);
    }
  }
} finally {
  await watch.close();
}
```

Three things there are load-bearing:

- **`retainedCount`** tells you the moment your state is complete.
- **`value === null` means removed**, deliberately distinguishable from an empty
  value. A watcher mirroring a cache has to tell those apart.
- **`laggedResumeFrom` is a value, not a rejection.** Re-watching from it is
  gapless. `shardMoved` is only a notice: the watch follows its shard to the
  new owner and carries on, with no change repeated or skipped.

`start` and `retained` are mutually exclusive — a resume already replays the
state a retained start shortcuts, so asking for both is refused rather than
resolved. A prefix watch reads **one shard**.

## Multi-shard streams

A subscription reads one shard. `subscribeSharded` opens one per shard, follows
each shard's own owner, and merges them:

```ts
const stream = await client.subscribeSharded("t1", "default", "orders");
console.log(`${stream.shards} shards`);

for (;;) {
  const item = await stream.nextEvent();
  if (item === null) break;
  if (item.event) {
    handle(item.event.payload);
  } else if (item.lostError) {
    // Surfaced rather than swallowed: the other shards carry on, so a consumer
    // that ignored this would be reading part of the stream while believing it
    // read all of it.
    alert(item.shard, item.lostError);
  } else if (item.recovered) {
    console.log(`shard ${item.shard} back`);
  } else if (item.shardMoved) {
    // Followed to its new owner; its records carry on from there.
    console.log(`shard ${item.shard} moved to ${item.shardMoved.nodeId}`);
  }
}
```

Resuming is a **map**, not a number, because offsets are per shard:

```ts
const positions = await stream.positions();   // { "0": 41n, "1": 12n, ... }
// ... later, after a restart
const resumed = await client.subscribeSharded(
  "t1", "default", "orders", undefined, positions,
);
```

A single offset carried across shards would replay on every shard but one.

:::caution[A shard that delivered nothing has no position]
`positions()` only lists shards that handed something over. On resume, a shard
not in the map starts wherever `start` says — the live tail by default — so
records published to it while you were away are missed. Pass
`start: "earliest"` alongside `resume` if that matters, or make sure every
shard has reported before you checkpoint.
:::

## Buffers are Buffers

Payloads and cache values come back as real `Buffer`s, not wrappers. `equals`,
`toString`, `Buffer.concat` and `deepStrictEqual` all behave:

```ts
if (event.payload.equals(expected)) { /* ... */ }
```

Offsets, counters and cache-watch offsets are `bigint`. Mixing them with
`number` throws, so `offset + 1n` rather than `offset + 1`.

## What is not wrapped

- **Idempotent producers** (`producerInit` / `publishIdempotent`) — they turn an
  ambiguous publish into one the broker recognises as a re-send and refuses to
  append twice. Surface over a primitive that already exists, not protocol
  work.

Marked in the conformance catalogue, so the binding reports it as unclaimed
rather than passing over it in silence.

## Conformance

TypeScript passes every required scenario in the
[client conformance catalogue](/felix/clients/overview/#the-conformance-suite),
and CI and the release pipeline are both gated on it. The suite runs against a
real three-node cluster — a redirect needs a broker that does not own the
shard, and one scenario kills the broker its client is connected to.

```bash
task ts:conformance    # build what it needs, run it, check the verdict
```
