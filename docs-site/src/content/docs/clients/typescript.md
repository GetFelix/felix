---
title: "TypeScript Client"
description: "Installing and using the Felix Node.js client: promises, typed errors, disposal, streams, queues, cache watches, multi-shard consumption, and the failure modes worth writing code for."
---

`felix-client` on npm is a napi-rs addon over the Rust crate of the same name.
Reconnection, redirect-following, retry classification and offset bookkeeping
live in the crate and are shared, so Node gets the same failover behaviour as
Rust. The name is identical on crates.io, PyPI and npm. See
[Choosing a Client](/clients/overview/) for why that choice was made.

It runs in Node, not in a browser, because browsers cannot speak QUIC. For a
browser, use [felix-gateway](/clients/browsers/), which relays WebSocket
messages to Felix and ships its own browser client.

## Installing

```bash
npm install felix-client
```

The binary ships as one package per platform, declared as optional
dependencies, so npm fetches only the one your machine needs. Nothing is
compiled at install time and no Rust toolchain is required. Linux (x86-64 and
arm64, glibc 2.28 or newer: Debian bookworm, RHEL 8, Amazon Linux 2023), macOS
(Intel and Apple silicon) and Windows x86-64 are covered. Alpine and other musl
systems are not.

To build it from the repository instead:

```bash
cd crates/sdk/felix-typescript
napi build --platform --release      # needs: npm i -g @napi-rs/cli@2
```

Without the napi CLI, `napi build` is mostly a rename: a plain `cargo build`
produces a loadable addon and the package finds it:

```bash
cargo build --release
```

Requires Node 18 or newer.

## One asynchronous surface

Python offers two surfaces because its synchronous one is the older idiom. Node
has one: a library must not block the event loop, so **every call returns a
`Promise`**. napi-rs runs the future on its own Tokio
runtime and settles the promise from there, which keeps the event loop free
while a publish is in flight.

```ts
import { Client } from "felix-client";

const client = await Client.connect("127.0.0.1:5000", "t1", { token, caFile });
try {
  await client.publish("t1", "default", "events", Buffer.from("hello"));
} finally {
  client.close();
}
```

`token` is a Felix token; [Getting and refreshing a
token](#getting-and-refreshing-a-token) covers where it comes from and how a
long-running client keeps it fresh.

### Getting and refreshing a token

A token lasts 15 minutes by default, and the broker checks it each time a stream
authenticates, including the streams a reconnect opens. [Getting a
token](/clients/overview/#getting-a-token) covers where one comes from. A
process that outlives one gives `connect` a `tokenProvider`, and the client asks
it for a token when it needs one. It asks lazily: when no token is held, when
the held one has passed two thirds of its life, or when a broker has just
refused it (one refetch per refused stream, retried only if the provider returns a different token, then the operation fails with the auth error). If a fetch fails while the held token is still valid, the client
keeps using that one and asks again about five seconds later.

The provider is a `(signal: AbortSignal) => string | Promise<string>`, passed as
the `tokenProvider` option of `Client.connect` or `withIdentity`. `token` is then
optional: leave it out to have the first connect call the provider (an empty
string counts as none), or pass a token you already hold to use that first.

**When it is called.** When a stream is about to authenticate and the client
holds no token, the held one is due for replacement, or a broker just refused
it. An idle client makes no calls, and there is no timer. The client never runs
two calls at once: streams that open together wait on one and share its token.
That is also what makes it safe to rotate a refresh token inside the provider.

**Threading.** The client calls it on the JavaScript thread, like any callback,
so do not block in it. A returned promise is awaited, so a slow answer does not
hold up the event loop. The provider does not keep the Node process alive.

**Failure.** The provider fails when it throws or rejects, returns something
that is not a non-empty string, or does not answer within 30 seconds. While the
held token is still valid the client keeps it and asks again about five seconds
later. Otherwise the operation that needed the token fails with `AuthError`
carrying the provider's message unchanged. A provider failure is final for that
operation: it is not retried, because each call can rotate a refresh token. The
next operation asks again. The 30 second timeout fails the operations waiting on
the call, but the call itself keeps running: the client never starts a second
one while it is outstanding, the next refresh joins it, and a late answer is
used.

**Cancellation.** The `signal` fires at the 30 second timeout and when the
client, its `withIdentity` clients and its open subscriptions and in-flight
calls are all gone; closing a client while a subscription or call still uses
the token source does not cancel it. A provider that ignores it still works: it
is not called again while pending, and its late answer is used. Pass it to
`fetch`, as the example does, so a hung request is cut off instead of left
running.

**Async variant.** `async` functions are the usual case, as in the example. A
plain function that returns the string works too.

The example does the control plane calls with `fetch` (Node 18 or later). The
refresh token works once: every refresh returns a replacement, so the provider
stores it before it returns. A 403 means the refresh token expired or was
revoked: run the exchange again (fresh OIDC login) and connect a new client; the
client keeps the last token until it expires.

```ts
const CONTROLPLANE = "https://controlplane:8443/v1/tenants/t1";

interface Minted {
  felix_token: string;
  refresh_token: string;
}

async function mint(path: string, body: unknown, signal: AbortSignal, bearer?: string): Promise<Minted> {
  const res = await fetch(CONTROLPLANE + path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(bearer ? { Authorization: `Bearer ${bearer}` } : {}),
    },
    body: JSON.stringify(body),
    signal,
  });
  if (!res.ok) throw new Error(`${path}: ${res.status}`); // 403: sign in again
  return (await res.json()) as Minted;
}

const exchanged = await mint(
  "/token/exchange",
  { requested: ["stream.publish"], resources: ["stream:t1/default/*"] },
  AbortSignal.timeout(10_000),
  oidcToken,
);

// The client never runs the provider twice at once, so no lock is needed.
let refreshToken = exchanged.refresh_token;

const client = await Client.connect("127.0.0.1:5000", "t1", {
  token: exchanged.felix_token,
  caFile,
  tokenProvider: async (signal) => {
    const minted = await mint("/token/refresh", { refresh_token: refreshToken }, signal);
    refreshToken = minted.refresh_token;
    return minted.felix_token;
  },
});
try {
  await client.publish("t1", "default", "events", Buffer.from("hello"));
} finally {
  client.close();
}
```

`client.withIdentity("t1", { tokenProvider })` takes the same kind of function for
one user, with that user's own refresh token. Refresh each chain from one place
at a time and keep the newest refresh token.

## Connecting

```ts
const client = await Client.connect(
  addrs,        // "host:port", or an array of them
  tenantId,
  {
    token,          // a Felix token; optional when tokenProvider is set
    tokenProvider,  // (signal: AbortSignal) => string | Promise<string>; see above
    serverName,     // the name the broker's certificate carries; defaults to "localhost"
    caFile,         // trust a specific CA; omit for the system trust store
    offerAlpn,      // offer the felix/1 ALPN; defaults to false
  },
);
```

An option the client does not know, or an extra positional argument, throws a
`TypeError` instead of being ignored.

One reachable address is enough. The client discovers the rest of the cluster
and will use brokers it was never told about. It is built on the Rust
`ClusterClient`, so it works the same against a single broker.
[Connecting to a cluster](#connecting-to-a-cluster) says what it does with
several.

Give a name rather than an IP where you can. The client resolves a name again
whenever no broker it knows answers, which is how it gets back in after a
rollout has moved every broker at once. A name that does not resolve when
you connect is an `InvalidArg` error.

TLS is always on. QUIC has no unencrypted mode, so there are two trust
choices: the platform trust store, or an explicit `caFile` for a self-signed
development broker. There is no "skip verification" switch.

Passing `offerAlpn: true` makes the client offer the `felix/1` ALPN, which
a broker running with `FELIX_TLS_REQUIRE_ALPN=true` insists on. It is off by
default because a broker older than ALPN support refuses a client that offers
it.

```ts
const client = await Client.connect(addrs, "t1", { token, caFile, offerAlpn: true });
```

### Acting for many users

A gateway serving many users can hold one client and give each user a client
that shares its connections:

```ts
const alice = await client.withIdentity("t1", { token: aliceToken });
try {
  await alice.publish("t1", "app", "alice-feed", Buffer.from("hello"));
} finally {
  alice.close();
}
```

Every stream `alice` opens authenticates with her token, so the broker checks
her requests against her grants, not the gateway's. She follows moved shards
and failed brokers like any client, over the gateway's connection to each
broker. Give `withIdentity` a fixed `token`, or a `tokenProvider` as above for a
long-lived handle. Closing `alice` leaves the gateway's connections open. The limits and the
certificate-bound token rules are the Rust client's; see
[Acting for many users](/clients/rust/#acting-for-many-users).

## Connecting to a cluster

In every language the client is a cluster client; see
[Choosing a Client](/clients/overview/#every-client-is-a-cluster-client).

```ts
const client = await Client.connect(
  ["felix-0:5000", "felix-1:5000", "felix-2:5000"],
  "t1",
  { token },
);
```

These are seeds: the client asks whichever answers which brokers exist and adds
them, and it keeps your seeds whatever the answer, so a stale one cannot leave
it worse off. List more than one so the first connection survives a seed being
down.

- **A broker fails.** A publish waiting on it fails once the connection has been
  silent for the idle timeout (6 s by default), with the connection already
  rebuilt from the brokers that remain. `publish` rejects; the next call goes to
  a live broker. An at-least-once publish resends instead, and may duplicate
  ([At-least-once may duplicate](#at-least-once-may-duplicate)).
- **A shard moves.** Publishes go to the broker that now leads it, and a
  subscription follows it ([Subscribing](#subscribing)).
- **A subscription's broker fails.** On a durable stream it resubscribes from
  the offset after the last one it delivered, and where it cannot it rejects
  rather than ending as though the stream had finished.
- **Knobs.** [Reconnecting](#reconnecting) sets the attempt count and a deadline. `await client.endpoints()` lists the brokers it would try, discovered
  ones included. `await client.refreshTopology()` asks the broker in use for the cluster's brokers again and resolves with how many it named (it rejects against a broker that predates discovery), and `await client.connectionsPerNode()` lists the connections held to each broker.

### Reconnecting

Publishes, opening and following subscriptions, commits, `publish_if`, state reads, cache calls and consumer-group calls retry under these options. A read, and a group describe or dead-letter listing, retries on any error that is not terminal. A write, including a group poll, which claims records, retries only on a broker's retryable refusal, which says nothing was applied, so a write that may have applied is never sent twice. The deadline covers re-routes and, for a call over every shard of a stream, all its shards together. Two options tune the retrying, and both default to the Rust client's own policy, so a client that sets neither behaves as it always did.

- **Attempts** is how many times a call tries to reach a broker before it gives up. Each attempt tries every broker the client knows. The default is 5, and the minimum is 1.
- **Deadline** is a ceiling on one call across every attempt and every wait between them. It is off by default, and it is checked between attempts, before each backoff sleep, so a call can last the deadline plus one attempt's own timeout. Attempt counts alone do not bound time: five attempts against a broker that takes its full publish timeout to answer is minutes.

Pass them as `reconnectAttempts` (a whole number) and `reconnectDeadline` (milliseconds) in the options object of `Client.connect`. The attempt count must be a whole number of at least 1 and the deadline between 1 ms and one year; anything else rejects with an `InvalidArgumentError`.

```ts
const client = await Client.connect(
  ["felix-0:5000", "felix-1:5000", "felix-2:5000"],
  "t1",
  { token, reconnectAttempts: 10, reconnectDeadline: 30_000 },
);
try {
  // Stops retrying once 30 s have passed, checked between attempts, and reports the last error.
  await client.publish("t1", "default", "orders", Buffer.from("hello"), undefined, "per_message", true);
} catch (err) {
  console.error("publish gave up:", err);
} finally {
  client.close();
}
```

A deadline shorter than one attempt's own timeout means no retry happens: the first attempt spends the whole budget and the call returns after trying once. Set it to cover at least one attempt plus the backoff you can afford, and leave it off if you would rather wait out a failover. A client from `withIdentity` inherits both.

The Rust page covers [clusters](/clients/rust/#clusters) in full. [Building Resilient Clients](/clients/resilient-clients/) shows reconnection, token refresh, retries, resuming and shutdown side by side in every language.

## Disposal

Every handle has an idempotent `close()` and implements `Symbol.asyncDispose`,
so on Node 24 and newer a `throw` releases it on the way out:

```ts
await using events = await client.subscribe("t1", "default", "events");
```

The package itself asks only for Node 18. The `await using` syntax is what
needs the newer runtime. On older Node, call `close()` in a
`finally`.

**`close()` cancels a read in flight rather than waiting for it.** A consumer
shutting down is almost always parked on `nextEvent`, and waiting for the read
it is cancelling would hang the path that needs to make progress.

## Publishing

```ts
await client.publish("t1", "default", "orders", payload);                          // acked
await client.publish("t1", "default", "orders", payload, undefined, "none");       // fire and forget
await client.publish("t1", "default", "orders", payload, Buffer.from(customerId)); // routed
```

Arguments are positional: `(tenantId, namespace, stream, payload, key?, ack?,
atLeastOnce?)`. Pass `undefined` to skip one.

`publish` resolves to the offset the record landed at, as a `bigint` like
`Event.offset`, or `null` when the broker acknowledged it before writing it (a
broker that acks on accept: `FELIX_ACK_ON_COMMIT=0`, or an in-memory stream), the stream has no log, the broker is
too old to say, or `ack` is `"none"`. Only the broker that owns the shard
acknowledges before writing; a publish forwarded through another broker is
answered after the write and has its offset, so one stream can return both.

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

### At-least-once may duplicate

By default a publish whose outcome was ambiguous is **reported, not re-sent**,
because nothing downstream can tell two copies apart.

```ts
await client.publish("t1", "default", "orders", payload, undefined, "per_message", true);
```

The record is then certain to land and **may land twice**. With a key it is
re-sent to the key's shard, so a duplicate lands next to the original. For
at-least-once *without* the duplicate, use an
[idempotent producer](#idempotent-producers).

### Publishing to a named shard

`publishOnShard` sends a batch to a shard you name instead of hashing a key, for
a producer that does its own partitioning:

```ts
const offset = await client.publishOnShard("t1", "default", "orders", 2, [
  Buffer.from("a"),
  Buffer.from("b"),
]);
```

The batch is one frame and the call resolves to the first record's offset, or
`null` as `publish` does. Arguments are `(tenantId, namespace, stream, shard,
payloads, ack?)`. A shard the stream does not have is refused with
`invalid_request` and nothing is written. It needs a broker that advertises
`FEATURE_EXPLICIT_SHARD`; against one that does not, it rejects without sending
anything. Like `publish`, it is not idempotent: a failure that may have applied
is reported, not re-sent.

### Batches

`publishBatch` sends several payloads as one frame to one shard. With a `key` they go to the shard the key hashes to, and without one to shard 0. It resolves to the first record's offset, or `null` as `publish` does. Arguments are `(tenantId, namespace, stream, payloads, key?, ack?)`:

```ts
const first = await client.publishBatch("t1", "default", "orders", ticks, Buffer.from(customerId));
```

`publishRecords` takes records that each carry their own key, splits them by shard and sends each shard's records to its owner. Failure is per shard and does not reject: the report says which records each piece held, and a failed piece does not stop the others.

```ts
const report = await client.publishRecords(
  "t1", "default", "orders",
  orders.map((order) => ({ key: Buffer.from(order.customerId), payload: Buffer.from(JSON.stringify(order)) })),
);
if (!report.ok) {
  for (const piece of report.shards) {
    if (piece.error !== null) console.error(`shard ${piece.shard} refused`, piece.records, piece.error);
  }
  await retry();
}
```

Each `ShardPublish` has the shard, the positions (in the order you passed) of the records it held, the offset its first record landed at, and `error`, the typed error a rejected call would carry. A piece is all or nothing. Without `atLeastOnce` a piece that may have been written is not sent again, so no record lands twice; pass `true` as the last argument to re-send such a piece after the reconnect backoff, and accept that a record may land twice. There is no order between shards, and none across two calls unless you await the first.

### Idempotent producers

At-least-once *without* the duplicate. `client.producer()` asks the broker for a
producer id and resolves to a `Producer`. It numbers its batches per shard, and
the shard's leader answers a number it already holds from memory instead of
appending, so a batch that got no answer can be sent again and lands once, across
reconnects and leader changes.

```ts
await using producer = await client.producer();
const offset = await producer.publish("t1", "default", "orders", Buffer.from("a"));
await producer.publishBatch("t1", "default", "orders", [Buffer.from("b"), Buffer.from("c")], Buffer.from("customer-7"));
await producer.publishBatches("t1", "default", "orders", [
  [Buffer.from("d"), Buffer.from("e")],
  [Buffer.from("f")],
]);
```

- `publish` and `publishBatch` take one sequence whatever the batch size and
  resolve to the first record's offset (a `bigint`), or `null` when the leader
  reported none. The optional last argument is the routing key, as for
  `client.publish`; without one the batch goes to shard 0. `publishBatches`
  sends several batches to shard 0 under consecutive sequences, and keeps them
  in flight together against a broker that pipelines.
- **A rejection leaves the batch in doubt.** Any rejection but
  `PublishRefusedError` means the sequence has not advanced and the batch may
  have landed. Call again with **the same payloads and key**: it goes out under
  the same sequence and lands once. A different batch rejects without being sent
  until the first one settles. After `publishBatches` fails, the batches already
  acknowledged stay acknowledged; call again with the same batches to send only
  the rest.
- **A refusal ends the producer on that stream.** `PublishRefusedError` means the
  broker will not append and a re-send cannot mend it. `err.reason` is
  `"sequence_gap"` (`err.expected` is the sequence it wanted),
  `"unknown_producer"`, `"sequence_expired"`, `"sequence_reused"`,
  `"offset_mismatch"` (`err.tail`) or `"not_leader"` (`err.nodeId`, `err.addr`).
  Every later call on that stream rejects with the same reason. Decide whether
  the last batch landed, then start a new producer, or call
  `abandonInDoubt(tenantId, namespace, stream)`, which numbers the stream afresh
  under a new id (every sequence on the stream restarts, whether or not a batch
  was in doubt). **A batch given up that way may land twice**, because it may
  have been appended before.
- **Abandoning a promise does not cancel the send.** Once a call has handed its
  batch to the producer it is sent and answered whether or not anyone waits.
  Racing a call against a timeout leaves the batch in doubt when the timeout
  wins, so settle it with the same call before sending anything else on that
  shard.
- Calls reach the broker in the order they were made. Await each call when order
  matters; calls started together, as with `Promise.all`, have no order between
  them.
- `producer.id` is the id the broker assigned, and `await producer.idFor(tenantId,
  namespace, stream)` the id a stream's batches go out under, which differs after
  `abandonInDoubt`.
- `close(timeoutMs?)` waits for the batches already handed over, then stops. A
  second close waits for the first to finish, then does nothing. After it every
  publish rejects with `FelixError`. A close that times out rejects, the producer
  stays closed, and a batch already sent may still land. The timeout also bounds
  the wait behind a publish that is parked on a full queue during an outage; if
  that wait runs out the producer is still open and the error says so. `await
  using` closes without a timeout, so during an outage it waits until the
  batches handed over are answered; call `close(timeoutMs)` first to bound it.

```ts
import { PublishRefusedError } from "felix-client";
import { setTimeout as sleep } from "node:timers/promises";

const producer = await client.producer();
try {
  for (let attempt = 1; ; attempt++) {
    try {
      const offset = await producer.publish("t1", "default", "orders", payload);
      console.log(`published exactly once at ${offset}`);
      break;
    } catch (err) {
      // The shard refuses this producer: sending the same batch again will
      // not change that.
      if (err instanceof PublishRefusedError) {
        throw new Error(`the broker refused the producer: ${err.reason}`);
      }
      // In doubt: the same call again is safe, and lands once.
      if (attempt === 5) throw err;
      console.warn(`publish in doubt; sending the same batch again: ${err}`);
      await sleep(1000);
    }
  }
} finally {
  await producer.close(10_000);
}
```

## Subscribing

```ts
const events = await client.subscribe("t1", "default", "events");
try {
  for (;;) {
    const event = await events.nextEvent();
    if (event === null) break;          // the subscription ended
    await handle(event.payload);
  }
} finally {
  await events.close();
}
```

`start` is `"latest"` (the default), `"earliest"`, or a `bigint` offset: **the
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
next record**, so keep the promise and await it again rather than calling
`nextEvent` afresh, or you will drop the record it was about to hand you.

```ts
import { setTimeout as sleep } from "node:timers/promises";

const TIMED_OUT = Symbol("timed out");
let pending: Promise<Event | null> | null = null;

// The next event, null once the subscription ends, or TIMED_OUT.
async function nextWithin(timeoutMs: number) {
  pending ??= events.nextEvent();
  const timer = new AbortController();
  try {
    const item = await Promise.race([pending, sleep(timeoutMs, TIMED_OUT, { signal: timer.signal })]);
    if (item !== TIMED_OUT) pending = null;   // after a timeout, pending is kept for next time
    return item;
  } catch (err) {
    pending = null;
    throw err;
  } finally {
    timer.abort();
  }
}
```
:::

`subscribe` reads one shard: pass a shard number after `start` for any but shard 0, or use [`subscribeSharded`](#multi-shard-streams) for all of them. `startOffset` and `liveOffset` on the handle say where the subscription began and where live delivery began when it opened, or `null` for an in-memory stream.

### Replaying from a time

`offsetForTime` finds the first offset on one shard whose record was appended at or after a time, given as microseconds since the Unix epoch in a `bigint`. Subscribe at it to replay from then:

```ts
const since = BigInt(Date.now() - 3_600_000) * 1000n;
const from = await client.offsetForTime("t1", "default", "orders", shard, since);
const replay = await client.subscribe("t1", "default", "orders", from ?? "latest", shard);
```

It resolves to `null` when nothing is that recent yet, which is why the example falls back to `"latest"`. A time older than every record the shard holds answers with the oldest. Times are the clock of the broker that led the shard at append, so if that clock stepped back the answer is near the first such record rather than exactly it. It needs a durable stream and the `stream.subscribe` permission.

### Reading without subscribing

`read` returns one page of a shard's committed records from an offset, up to an optional end, without holding a subscription:

```ts
let next = 0n;
while (next < end) {
  const page = await client.read("t1", "default", "orders", shard, next, end, 500);
  for (const record of page.records) await handle(record.payload);
  next = page.nextOffset;
}
```

`nextOffset` is where the next page starts. Offsets that hold no record are passed over, so it is not always the last offset plus one. A `maxRecords` of `0` takes the broker's cap. A `from` below what retention kept, or past the tail, is a `CursorError`. Each `StreamRecord` has the `offset`, `payload`, `publisher` (when the broker recorded one) and `timestampMicros`. Only the shard's leader answers, and it needs the `stream.subscribe` permission.

### Offsets are how you notice a drop

Subscriber queues shed under the default policy rather than blocking the
publisher. On a durable stream a reader that falls behind does not lose
records: the broker or this client ends the subscription at the first drop,
and the subscription resubscribes after the last record it delivered,
catching up from the log. A shard reader of a sharded subscription reports the
shard lost and recovered while it does.

Against a broker that predates that, or on an in-memory stream, a subscriber
can still silently miss records. On a durable stream every
event carries its log offset, and **a jump in them is a drop**. The exception is
a new leader's generation-start record, which takes an offset; the next event
reports it in `skippedBefore`:

```ts
let expected: bigint | null = null;
for (;;) {
  const event = await events.nextEvent();
  if (event === null) break;
  if (event.offset !== null) {          // null on an in-memory stream
    const from = event.offset - event.skippedBefore;
    if (expected !== null && from !== expected) {
      console.warn(`dropped ${from - expected} records`);
    }
    expected = event.offset + 1n;
  }
  await handle(event.payload);
}
```

### A consumer that survives a restart

```ts
import { setTimeout as sleep } from "node:timers/promises";

interface Checkpoint {
  load(): Promise<bigint | null>;                // null on a cold start
  save(offset: bigint): Promise<void>;
}

async function run(client: Client, checkpoint: Checkpoint) {
  const saved = await checkpoint.load();
  let start: StartPosition = saved === null ? "earliest" : saved + 1n;

  for (;;) {
    try {
      const events = await client.subscribe("t1", "default", "events", start);
      try {
        for (;;) {
          const event = await events.nextEvent();
          if (event === null) break;
          if (event.offset === null) throw new Error("events is not a durable stream");
          await handle(event.payload);
          await checkpoint.save(event.offset);
          start = event.offset + 1n;
        }
      } finally {
        await events.close();
      }
    } catch (err) {
      if (err instanceof ConnectionError) {
        await sleep(1000);                       // retryable; resume from `start`
      } else if (err instanceof CursorError) {
        // Retention discarded the offset. Accept the gap and say so. Silently
        // restarting at the tail would lose records without telling anyone.
        console.error(`checkpoint ${start} is past retention; restarting at earliest`);
        start = "earliest";
      } else {
        throw err;
      }
    }
  }
}
```

## Errors you can act on

```ts
import { AuthError, FelixError, NotFoundError, OutcomeUnknownError } from "felix-client";

try {
  await client.publish("t1", "default", "orders", payload);
} catch (err) {
  if (err instanceof OutcomeUnknownError) await reconcile();          // it may have been written
  else if (err instanceof FelixError && err.retryable) await retry(); // nothing was written
  else if (err instanceof AuthError) await giveUp();                  // retrying grants no permission
  else if (err instanceof NotFoundError) await createStream();
  else throw err;
}
```

| Class | What it means | `retryable` without a broker code |
| --- | --- | --- |
| `ConnectionError` | broker unreachable, the connection died mid-call, or the broker is shutting down (`draining`) | `true` |
| `ShardUnavailableError` | nobody can serve the shard right now, usually because it is moving (`shard_unavailable`), or another broker owns it (`not_leader`); nothing was written | `true` |
| `OverloadedError` | the broker is shedding load (`overloaded`); nothing was written | `true` |
| `OutcomeUnknownError` | the write may or may not have happened (`quorum_timeout`, `leadership_lost`, `unacknowledged`, or any error sent as `outcome_unknown`) | `false` |
| `AuthError` | token rejected or revoked, or missing the permission (`unauthenticated`, `forbidden`, `revoked`) | `false` |
| `NotFoundError` | no such tenant, namespace, stream or cache (`not_found`) | `false` |
| `CursorError` | the start offset is gone; retention discarded it | `false` |
| `PublishRefusedError` | an idempotent producer's batch was refused and nothing written (`reason`: `sequence_gap`, `unknown_producer`, ...); see [idempotent producers](#idempotent-producers) | `false`; it ends the producer on that stream |
| `InvalidArgumentError` | a bad argument to this client | `false` |
| `FelixError` | the base, and anything else (`invalid_request`, `limit_exceeded`, a code this client does not know) | `false` |

Every error carries what the broker said about it:

- `code`: the broker's [error code](https://github.com/GetFelix/felix/blob/main/docs/protocol.md#error-codes),
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
happened in the client. Then the class is chosen from the message.

`err.kind` is this client's own name for the class (`FELIX_CONNECTION`,
`FELIX_SHARD_UNAVAILABLE`, …), set whether or not the broker sent a code, for
code that would rather switch than test `instanceof`. Never match on the message;
it is prose and will be reworded.

## Queues

Records are **pulled**, because only the consumer knows when it has capacity;
each is claimed by one member until settled; an unsettled record comes back.

```ts
for (;;) {
  const records = await client.groupPoll(
    "t1", "default", "orders", shard, "billing",
    32,        // maxRecords
    5000,      // waitMs: a long poll; an empty array is an answer, not an error
  );

  for (const record of records) {
    try {
      await charge(record.payload);
    } catch (err) {
      console.error(`order at ${record.offset} failed:`, err);
      // Back to the queue now, rather than after the visibility timeout.
      await client.groupNack("t1", "default", "orders", shard, "billing", record);
      continue;
    }
    await client.groupAck("t1", "default", "orders", shard, "billing", record.offset);
  }
}
```

`groupNack` takes the record, not its offset, because it names that delivery:
once the claim has lapsed and the record has gone out again, the nack is
refused with `stale_claim` rather than taking it from the consumer now holding
it.

`record.attempts` counts deliveries **including this one**, so `1` is a first
attempt and anything higher is a redelivery. It is worth branching on before doing
anything expensive or side-effecting:

```ts
if (record.attempts > 3) {
  await quarantine(record);
  // Settle it; it is not coming back.
  await client.groupAck("t1", "default", "orders", shard, "billing", record.offset);
}
```

**A group is bound to one shard.** Consuming a multi-shard stream means polling
each shard's group; `streamShards` says how many there are. Only the shard's leader
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

You can also give up on a claimed record yourself. `groupDeadLetter` takes the offset, lists the record among the group's dead letters and finishes it, as the group does once a record runs out of attempts. `groupRedrive` puts it back. It needs a broker that supports claim control.

```ts
await client.groupDeadLetter("t1", "default", "orders", shard, "billing", record.offset);
```

### Retrying later

`groupNackAfter` hands a record back to be redelivered after a delay, in milliseconds, instead of at once. It names the delivery like `groupNack`. A delay of `0` is a plain nack. The broker caps the delay (`FELIX_GROUP_MAX_VISIBILITY_MS`), and until it passes the record still counts against the group's in-flight cap. The delay is the leader's memory, so a failover redelivers the record sooner.

```ts
await client.groupNackAfter("t1", "default", "orders", shard, "billing", record, 30_000);
```

`groupPoll` takes the claim length for a poll as its last argument, `visibilityMs`, instead of the broker's visibility timeout. A broker that cannot honour it refuses the poll rather than claiming for its own timeout.

### Long work

A claim stands for the broker's visibility timeout. A consumer still working on a record asks for more time with `groupExtend`, which takes the record and a number of milliseconds and resolves with how long the claim now stands. That is less than asked when the broker caps it (`FELIX_GROUP_MAX_VISIBILITY_MS`):

```ts
for (const record of records) {
  while (!(await finished(record))) {
    await client.groupExtend("t1", "default", "orders", shard, "billing", record, 60_000);
    await workAWhile(record);
  }
  await client.groupAck("t1", "default", "orders", shard, "billing", record.offset);
}
```

Like a nack, an extension names its delivery. Once the claim has lapsed and the record has gone out again it is refused with `stale_claim`, even if your worker is still running.

### Creating, moving and deleting a group

A group starts at offset 0 on its first poll unless it was created first. These calls need the `group.manage` permission:

```ts
// Left alone if the group already exists, so safe on every start.
const position = await client.groupCreate("t1", "default", "orders", shard, "billing", "latest");
console.log(position.offset, position.moved);

// Replay everything the shard still holds. A start is "latest", "earliest" or a bigint offset.
await client.groupSeek("t1", "default", "orders", shard, "billing", "earliest");

const info = await client.groupDescribe("t1", "default", "orders", shard, "billing");
console.log(info.committed, info.tail, info.inFlight, info.lag);

// false if there was no such group.
const existed = await client.groupDelete("t1", "default", "orders", shard, "billing");
```

`groupCreate` and `groupSeek` resolve with a `GroupPosition`: the offset the group resumes from, and `moved`, which is `false` when `groupCreate` found the group already there. A seek voids the claims standing when it lands, so an ack for a record the group now owes fails with `stale_claim`. `groupDescribe` resolves with a `GroupInfo`; `committed` is `null` for a group with no cursor, and `inFlight` and `owed` are the shard leader's memory, so they start again from zero when the leader changes. Every count is a `bigint`.

### The whole stream at once

The administration calls have stream-wide forms that act on every shard in shard order and answer once per shard: `groupCreateStream` and `groupSeekStream` resolve with a `GroupPosition[]`, `groupDescribeStream` with a `GroupInfo[]` indexed by shard, and `groupDeleteStream` with whether any shard had a group to delete.

```ts
await client.groupCreateStream("t1", "default", "orders", "billing", "latest");
const lag = (await client.groupDescribeStream("t1", "default", "orders", "billing"))
  .reduce((sum, info) => sum + info.lag, 0n);
```

They send one request per shard, not one cut across them, so `"latest"` is each shard's tail when its own request lands. An offset is refused for a stream with more than one shard, because offsets are per shard. A failure partway leaves the earlier shards changed; each call is safe to repeat.

To consume every shard through one handle, `groupSharded` returns a `ShardedGroupHandle`. `poll` takes from the next shard that has records and tags each with its shard; `ack`, `nack`, `nackAfter`, `extend` and `deadLetter` send the settle to that shard. The shards are not merged into one order.

```ts
const group = await client.groupSharded("t1", "default", "orders", "billing");
for (const record of await group.poll(32)) {
  await charge(record.payload);
  await group.ack(record);
}
```

## Cache and counters

```ts
await client.cachePut("t1", "default", "sessions", "user-abc", data, 3600);  // ttlSeconds
const value = await client.cacheGet("t1", "default", "sessions", "user-abc");      // Buffer | null
const removed = await client.cacheDelete("t1", "default", "sessions", "user-abc"); // Buffer | null
```

`ttlSeconds` is a number of seconds and may be fractional, so `0.5` is half a
second. Leave it out and the entry has no time-to-live. `cacheGet` returns `null`
for a key that is missing or expired. `cacheDelete` returns the value it
removed, or `null` if the key held nothing, so you can tell a delete that did
something from one that did not.

Counters live beside the cache and use the same scoping:

```ts
const total = await client.counterAdd("t1", "default", "limits", "user:42:reqs", 1); // sum after the add
const current = await client.counterGet("t1", "default", "limits", "user:42:reqs");  // number | null
```

`counterAdd` takes a signed delta and returns the sum including it.
`counterGet` returns `null` for a counter that was never written, which is not
the same as zero. A retry after a lost acknowledgement counts twice. Counters
need a durable broker that advertises `FEATURE_COUNTERS`.

Cache and counter calls go to the key's shard owner when the client knows it,
like a publish. If the broker the client entered by goes away, the next call
moves to another one: a read (a get) is asked again there, while a write that
may have landed (a put, delete or add) returns its error with the client
already moved, and is not sent twice. `streamShards` moves the same way.

### Conditional writes

`cachePutIf` stores a value only if the key is absent or still at the version
you read, and `cacheDeleteIf` removes it only at that version. The check and
the write are one step on the key's owner, so two clients racing for the same
key cannot both win. `cacheGetVersioned` returns the value with its version, or
`null`.

```ts
import { CacheCondition } from "felix-client";

async function holdLease(): Promise<void> {
  // Take a lease if nobody holds it.
  const taken = await client.cachePutIf(
    "acme", "prod", "leases", "endpoint-7", Buffer.from("worker-1"),
    CacheCondition.absent(), 30, // ttlSeconds
  );
  if (!taken.applied || taken.version === null) {
    throw new Error(`held already, at version ${taken.version}`);
  }
  let version = taken.version;

  // Renew it only if it is still ours.
  const renewed = await client.cachePutIf(
    "acme", "prod", "leases", "endpoint-7", Buffer.from("worker-1"),
    CacheCondition.version(version), 30,
  );
  if (renewed.applied && renewed.version !== null) version = renewed.version;

  // Release it only if nobody took it over.
  await client.cacheDeleteIf("acme", "prod", "leases", "endpoint-7", version);
}
```

For a read-modify-write, read the version, write with `CacheCondition.version`,
and read again if the write was refused:

```ts
import { CacheCondition } from "felix-client";

for (;;) {
  const current = await client.cacheGetVersioned("acme", "prod", "config", "endpoints");
  if (current === null) break;
  const edited = Buffer.concat([current.value, Buffer.from(",10.0.0.9")]);
  const done = await client.cachePutIf(
    "acme", "prod", "config", "endpoints", edited, CacheCondition.version(current.version),
  );
  if (done.applied) break;
}
```

A refusal is a result, not an error: `applied` is `false` and `version` is the
key's current version (`null` when it has none). An expired entry counts as
absent. A key's version only grows and is never reused. Versions are `bigint`,
and `CacheCondition.version` takes a `bigint` or a `number`. The calls need a
broker that advertises `FEATURE_CACHE_CONDITIONAL`; against an older one they
reject without sending anything.

A conditional write is never sent again after its answer was lost: the promise
rejects, and a retry of a put that had applied would be refused by the version
it wrote. Read the key with `cacheGetVersioned` to find out whether it landed.
Abandoning the promise after the request went out leaves the write in doubt the
same way, so read back before retrying.
See [Conditional writes](/features/cache/#9-conditional-writes).

## Cache watches

Watches can resume by offset and report loss explicitly, which makes the cache
usable for state synchronisation.

```ts
function apply(roster: Map<string, Buffer>, { key, value }: CacheChange) {
  if (value === null) roster.delete(key);   // a delete is a change with no value
  else roster.set(key, value);
}

// Mirrors the room into `roster`. Resolves to the offset to watch again from
// after a lag, or null once the watch ends.
async function mirror(roster: Map<string, Buffer>): Promise<bigint | null> {
  const watch = await client.watchCache(
    "t1", "default", "sessions",
    undefined,        // key, or a prefix (below)
    "room:42:",       // prefix
    undefined,        // start offset
    true,             // retained
  );
  try {
    // Retained values arrive first, and the count says exactly how many. 0n is a
    // definite answer (the prefix is empty), not a silence to wait through.
    for (let i = 0n; i < (watch.retainedCount ?? 0n); i++) {
      const item = await watch.recv();
      if (item?.change == null) throw new Error("the watch ended before its retained values");
      apply(roster, item.change);
    }

    // State is now complete. Everything after this is live.
    for (;;) {
      const item = await watch.recv();
      if (item === null) return null;
      if (item.laggedResumeFrom !== null) {
        // Not an error: the watch did its job by saying so. Offsets on a filtered
        // watch are sparse, so loss cannot be inferred the way a stream
        // subscriber infers it. This is the only signal.
        return item.laggedResumeFrom;
      }
      // Otherwise shardMoved: the watch follows the shard to its new broker.
      if (item.change !== null) apply(roster, item.change);
    }
  } finally {
    await watch.close();
  }
}
```

Three things there matter:

- **`retainedCount`** tells you the moment your state is complete.
- **`value === null` means removed**, deliberately distinguishable from an empty
  value. A watcher mirroring a cache has to tell those apart.
- **`laggedResumeFrom` is a value, not a rejection.** Re-watching from it is
  gapless. `shardMoved` is only a notice: the watch follows its shard to the
  new owner and carries on, with no change repeated or skipped. A watch whose
  broker dies is reopened the same way, without a notice, within the client's
  reconnect deadline; if no broker takes it, the watch ends.

`start` and `retained` are mutually exclusive. A resume already replays the
state a retained start shortcuts, so asking for both is refused. A prefix watch
with `watchCache` reads **one shard**; the next section covers a cache with
several.

### A prefix across every shard

Keys sharing a prefix hash to different shards, and a watch reads one. `watchCacheSharded` opens a prefix watch per shard, follows each shard to its owner and merges them into one handle. It fails if any shard cannot be opened, because a watch covering three shards of four looks complete to everything downstream.

```ts
const watch = await client.watchCacheSharded("t1", "default", "sessions", "room:42:");
try {
  for (;;) {
    const item = await watch.recv();
    if (item === null) break;
    if (item.change !== null) {
      console.log(item.shard, item.change.key);
    } else if (item.shardClosed) {
      // The other shards carry on, so this is part of the prefix gone quiet.
      console.error(`shard ${item.shard} stopped`);
    }
  }
} finally {
  await watch.close();
}
```

Ordering is per key: a key lives on one shard, so its changes arrive in write order, and keys on different shards have no order between them. Each `offset` belongs to its own shard's log. Losing a shard is an item and not silence: `laggedResumeFrom` (that shard fell behind), `shardMoved` (it is being followed to a new owner) and `shardClosed` (it ended) each name the `shard`.

`resumeOffsets()` is one offset per shard, never ahead of what you have handled. Pass it back as the `resume` argument to continue gaplessly. A shard still delivering its state resumes at `0n`, so no key is missed.

A `CacheWatchHandle` also exposes `resumeFrom`, the `fromOffset` that continues it without a gap, and `moves`, how often it followed its shard to a new owner; both can be read while a `recv()` is in flight.

Pass `true` as the last argument to retain: every matching key's current value arrives first, from every shard, and `stateComplete` marks the moment all of them have. `retainedCount` is the total to expect. A retained watch cannot also resume.

```ts
const roster = await client.watchCacheSharded("t1", "default", "sessions", "room:42:", undefined, true);
```

## Atomic commits

`commit` writes an event and the state it changes as one record on the shard
an entity key routes to. A subscriber, a consumer group and `stateGet` all see
the whole commit or none of it, at the same offset.

```ts
import { CommitOp, EventCountError, NotOnOwningShardError } from "felix-client";

const stream = "order-events";
const receipt = await client.commit("acme", "orders", Buffer.from("order-42"), [
  CommitOp.enqueue(stream, '{"type":"placed","id":42}'),
  CommitOp.put(stream, "order-42", '{"status":"placed"}'),
  CommitOp.delete(stream, "cart-42"),
]);
receipt.offset; // bigint: where the event is read, and the state's version

const state = await client.stateGet("acme", "orders", stream, Buffer.from("order-42"), "order-42");
state.version === receipt.offset; // true
```

| Call | Resolves with |
| --- | --- |
| `commit(tenantId, namespace, entityKey, ops)` | `{ offset: bigint }` |
| `stateGet(tenantId, namespace, stream, entityKey, key)` | `{ value: Buffer \| null, version: bigint \| null, asOf: bigint \| null }` |
| `CommitOp.publish(stream, payload)`, `CommitOp.enqueue(queue, payload)` | the commit's one event, for subscribers or for consumer groups (the same record: a queue is a stream read through a group) |
| `CommitOp.put(stream, key, value)`, `CommitOp.delete(stream, key)` | state changes, applied in order |

The builders return plain objects (`{ op: "put", stream, key, value }` and so
on), so writing the literal works as well.

A commit is refused before anything is sent when it cannot be one record:

```ts
try {
  await client.commit("acme", "orders", Buffer.from("order-42"), [
    CommitOp.publish("order-events", "placed"),
    CommitOp.put("inventory", "sku-1", "3"), // another stream
  ]);
} catch (err) {
  if (err instanceof NotOnOwningShardError) console.log(err.index, err.stream, err.owner);
  else if (err instanceof EventCountError) console.log(err.count);
  else throw err;
}
```

Both extend `CommitError`, which a broker without commits also rejects with.
Failures after the commit was sent are the ordinary classes above; a commit is
**not idempotent**, so an `OutcomeUnknownError` means it may already be
written.

**What atomic covers:** every part of one commit, on one shard's log, across a
failover. **What it does not:** another stream or shard, a Felix cache (a
commit's state is the stream shard's own, read with `stateGet`), or a retry.
State is rebuilt from the retained log, so keep an entity stream's retention
long enough. The stream must be durable, and in a cluster the operator must
finalize the `atomic_commit` fleet feature first. The full semantics are in
[`docs/atomic-commit.md`](https://github.com/GetFelix/felix/blob/main/docs/atomic-commit.md).

### Conditional commits and named shards

`commitIf` and `publishIf` write only if the shard's next offset is the one you
expect, a compare-and-set on the whole shard. They suit a single writer that
must not append after it has been replaced.

```ts
let next = 0n; // the offset this writer expects to write next
for (const tick of ticks) {
  const result = await client.publishIf("acme", "games", "match-7", [tick], next);
  if (!result.written) {
    throw new Error(`another writer has the stream; its next offset is ${result.tail}`);
  }
  next = result.offset + 1n;
}
```

A `ConditionalWrite` is `{ written: true, offset, tail: null }` or
`{ written: false, offset: null, tail }`, where `tail` is the shard's next
offset. A refusal is a result, not an error, and writes nothing. `publishIf`
takes at least one payload and routes by the optional `key`, or to shard 0
without one. `commitIf(tenantId, namespace, entityKey, ops, expectedOffset)` is
the same check on a commit. Both need a broker that advertises
`FEATURE_PUBLISH_CONDITIONAL`. Do not resend one whose answer was lost: it
rejects, and the client does not send it again either. Read the shard at the
expected offset to find out whether it landed.

`commitOnShard`, `commitIfOnShard`, `publishIfOnShard` and `stateGetOnShard`
take a shard number instead of hashing a key, for a writer that keeps one shard
per partition:

```ts
import { CommitOp } from "felix-client";

const ops = [CommitOp.publish("ticks", "tick-9"), CommitOp.put("ticks", "district", "quiet")];
// 0n: the shard's next offset, as the writer last saw it.
const written = await client.commitIfOnShard("acme", "city", 2, Buffer.from("district"), ops, 0n);
const state = await client.stateGetOnShard("acme", "city", "ticks", 2, "district");
```

A keyed `stateGet` reads the shard the key hashes to, so it does not find state
committed to another named shard; read it with `stateGetOnShard`. A shard the
stream does not have is refused and nothing is written. These need a broker
that advertises `FEATURE_EXPLICIT_SHARD`.

## Multi-shard streams

A subscription reads one shard. `subscribeSharded` opens one per shard, follows
each shard's own owner, and merges them:

```ts
const sharded = await client.subscribeSharded("t1", "default", "orders");
console.log(`${sharded.shards} shards`);

try {
  for (;;) {
    const item = await sharded.nextEvent();
    if (item === null) break;
    if (item.event) {
      await handle(item.event.payload);
    } else if (item.lostError) {
      // Surfaced rather than swallowed: the other shards carry on, so a consumer
      // that ignored this would be reading part of the stream while believing it
      // read all of it.
      console.error(`shard ${item.shard} lost: ${item.lostError}`);
    } else if (item.recovered) {
      console.log(`shard ${item.shard} back`);
    } else if (item.shardMoved) {
      // Followed to its new owner; its records carry on from there.
      console.log(`shard ${item.shard} moved to ${item.shardMoved.nodeId}`);
    } else if (item.streamDeleted) {
      // The stream is gone; this shard is not reopened.
      console.log(`shard ${item.shard}: stream deleted`);
    }
  }
} finally {
  await sharded.close();
}
```

Resuming is a **map**, not a number, because offsets are per shard:

```ts
const positions = await sharded.positions();  // { "0": 41n, "1": 12n, ... }
// ... later, after a restart
const resumed = await client.subscribeSharded(
  "t1", "default", "orders", undefined, positions,
);
```

A single offset carried across shards would replay on every shard but one.

:::caution[A shard that delivered nothing has no position]
`positions()` only lists shards that handed something over. On resume, a shard
not in the map starts wherever `start` says (the live tail by default), so
records published to it while you were away are missed. Pass `"earliest"` as
`start` alongside `resume` if that matters, or make sure every shard has
reported before you checkpoint:

```ts
await client.subscribeSharded("t1", "default", "orders", "earliest", positions);
```
:::

## Buffers are Buffers

Payloads and cache values come back as real `Buffer`s, not wrappers. `equals`,
`toString`, `Buffer.concat` and `deepStrictEqual` all behave:

```ts
const event = await events.nextEvent();
if (event?.payload.equals(expected)) { /* ... */ }
```

Stream offsets, group offsets and cache-watch offsets are `bigint`. Mixing
them with `number` throws, so `offset + 1n` rather than `offset + 1`. Counter
values are plain `number`s, exact up to `Number.MAX_SAFE_INTEGER`.

## What is not wrapped

The [Rust client](/clients/rust/) has more than this binding wraps. What is
left out, and why:

- **Subscription counters.** `ClusterSubscription::moves`, `reconnects` and
  `stream_deleted` have no accessor on a subscription, sharded or not. A read
  in flight holds the handle, so these need their own shared state. A
  follow-up is planned; a move still shows up as a `ShardMoved` item on a
  sharded subscription. A cache watch does expose `moves` and `resume_from`.
- **The single-connection `Client` and `Publisher` layer** (`topology`,
  `stream_routing`, `shard_owners`, `inspect_shard`, `supports_*`,
  `publish_batch_binary*`, `connect_with_transport`). The binding wraps
  `ClusterClient`, which owns those connections and runs the feature checks
  itself.
- **`hand_off_*` and `Handed` on the idempotent producer.** They hand back raw
  futures; `publish_batches` is the iterator-shaped equivalent.
- **`ClientConfig` options** such as `publishers` and `timestamps`. They are
  connection configuration, set once, not calls.
- **Reconnect backoff.** `ReconnectPolicy.backoff` and `max_backoff` have no
  option here yet. The client uses their defaults; [Reconnecting](#reconnecting)
  sets the attempt count and the deadline.

## Conformance

TypeScript passes every required scenario in the
[client conformance catalogue](/clients/overview/#the-conformance-suite),
and CI and the release pipeline are both gated on it. The suite runs against a
real three-node cluster: a redirect needs a broker that does not own the
shard, and one scenario kills the broker its client is connected to.

```bash
task ts:conformance    # build what it needs, run it, check the verdict
```
