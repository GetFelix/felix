# felix-client for Node.js and TypeScript

Node bindings for Felix, built as a wrapper over the Rust client rather than a
reimplementation of the protocol.

That distinction is the point. Reconnection, redirect-following, retry
classification and offset bookkeeping are hard to get right and expensive to
get wrong — a second implementation is a second set of subtle bugs in exactly
the places that matter. Here they exist once, in `felix-client`, and every
language binds to them. The Python binding is built on the same reasoning and
exposes the same surface.

```bash
npm install felix-client
```

The binary ships as one package per platform, declared as optional
dependencies, so npm fetches only the one your machine needs. Nothing is
compiled at install time. Linux (x86-64 and arm64, glibc), macOS (Intel and
Apple silicon) and Windows x86-64 are covered. Node 18 or newer.

Full documentation: https://gabloe.github.io/felix/clients/typescript/

## One surface, and it is asynchronous

Python offers two surfaces because its sync one is the older idiom. Node has no
such split: blocking the event loop is not something a library may do, so every
call here returns a `Promise`. napi-rs runs the future on its own Tokio runtime
and settles the promise from there, which keeps the event loop free while a
publish is in flight.

```ts
import { Client } from "felix-client";

const client = await Client.connect("127.0.0.1:5000", "t1", token, "localhost", caFile);

await client.publish("t1", "default", "events", Buffer.from("hello"));

const events = await client.subscribe("t1", "default", "events");
for (;;) {
  const event = await events.nextEvent();
  if (event === null) break;
  console.log(event.payload.toString(), event.offset);
}
await events.close();
```

## What it wraps

Everything the Python binding wraps, with one exception noted below.

| | |
|---|---|
| Publish | `publish(tenant, ns, stream, payload, key?, ack?, atLeastOnce?)` |
| Subscribe | `subscribe(...)` → `nextEvent()`, `close()`, `closed` |
| Sharded subscribe | `subscribeSharded(..., start?, resume?)` → `nextEvent()`, `positions()`, `shards` |
| Stream shape | `streamShards(...)`, `endpoints()` |
| Cache | `cachePut` (with TTL), `cacheGet`, `cacheDelete` |
| Counters | `counterAdd`, `counterGet` |
| Cache watches | `watchCache(..., key?, prefix?, start?, retained?)` → `recv()`, `retainedCount` |
| Consumer groups | `groupPoll`, `groupAck`, `groupNack`, `groupDeadLetters`, `groupDiscard`, `groupRedrive` (each follows the broker's redirect to the shard's leader) |

Every handle has an idempotent `close()` and implements `Symbol.asyncDispose`,
so on Node 24 and newer a `throw` releases it on the way out:

```ts
await using events = await client.subscribe("t1", "default", "events");
```

The package itself asks only for Node 18 — `await using` is the syntax that
needs the newer runtime, not the disposal.

### Routing keys decide the shard

`publish` takes an optional key. Without one every record lands on shard 0, so
a multi-shard stream behaves like a single-shard one:

```ts
await client.publish("t1", "default", "orders", payload, Buffer.from(customerId));
```

Records sharing a key share a shard and stay ordered with respect to each
other. Records with different keys do not, once a stream has more than one
shard.

### Offsets are how you notice a drop

Subscriber queues shed under the default policy rather than blocking the
publisher, so a subscriber can silently miss records. On a durable stream each
delivered event carries its log offset, and a jump in them is a drop —
which is why `event.offset` is worth reading even when you do not resume from
it. The one gap that is not a drop is a promoted leader's generation-start
record, and the event after it says so: `offset - previous - 1n - skippedBefore`
records were dropped.

### A sharded subscription surfaces shard trouble rather than hiding it

`subscribeSharded` merges every shard, and each item says which shard it came
from. A lost shard arrives as an item of its own and does not disturb the
others: they keep delivering, and the lost one resumes from its own last offset
so nothing is skipped. Per-shard ordering is all a sharded stream has, and the
handle does not pretend otherwise.

### TLS is not optional

QUIC has no unencrypted mode, so there are two trust choices and no third: the
platform trust store (omit `caFile`) or an explicit CA file (what a self-signed
development broker needs). There is deliberately no "skip verification" switch
— it is the one setting that silently turns a secure deployment insecure, and a
CA file covers the development case without it.

### At-least-once publishing duplicates, and says so

By default a publish whose outcome was ambiguous — the broker may or may not
have written it before the connection went — is **reported, not re-sent**.
Nothing downstream can tell two copies apart, so re-sending silently changes
the delivery guarantee.

```ts
await client.publish("t1", "default", "events", payload, undefined, "per_message", true);
```

That is the opt-in. The record is then certain to land and **may land twice**.
It cannot be combined with a routing key: the re-send path does not carry one
yet, and honouring the key on the first attempt but not the re-send would move
the duplicate to a different shard, so the combination is refused rather than
resolved.

### Errors carry an identity, not just a message

```ts
try {
  await client.publish("t1", "default", "events", payload);
} catch (err) {
  if (err instanceof OutcomeUnknownError) reconcile(); // it may have been written
  else if (err.retryable) retry();                     // nothing was written
  else if (err instanceof AuthError) giveUp();         // no amount of retrying grants a permission
}
```

`FelixError` is the base; `ConnectionError`, `ShardUnavailableError`,
`OverloadedError`, `OutcomeUnknownError`, `AuthError`, `NotFoundError`,
`CursorError` and `InvalidArgumentError` are the branches, mirroring the Python
binding's exceptions. A broker that sends error codes picks the class, and the
error carries its `code`, `retry` class and `detail`, as the Python exceptions
do; from an older broker they are `undefined` and the class comes from the
message. Each also carries a stable `kind` (`FELIX_AUTH`, …) for code that
would rather switch than test `instanceof`. Never match on the message — it is prose, and it will be
reworded.

## What is not wrapped

- **Idempotent producers** (`producer_init` / `publish_idempotent`). They turn
  an ambiguous publish into one the broker can recognise as a re-send and
  refuse to append twice — at-least-once without the duplication. Surface over
  a primitive that already exists, not protocol work.

## Conformance

This binding runs the client conformance suite and passes every required
scenario in the catalogue, which is what makes "the Node client behaves like
the Rust one" a checked claim rather than an intention.

The suite is not a mock. `felix-cluster client-fixture` starts a real
three-node cluster, and the tests drive it: a redirect needs a broker that does
not own the shard, and `reconnect.survives_broker_loss` kills the broker its
client is connected to. Each test names the scenarios it demonstrates; the run
writes a results document, and `felix-conformance verify` checks it against the
catalogue. A test that fails, errors, or never runs becomes a non-passing
outcome, so a green run that quietly skipped a required scenario is still
reported as non-conformant.

```bash
task ts:conformance          # build what it needs, run it, verify the verdict
task conformance:scenarios   # the catalogue itself
```

Two optional scenarios are recorded as skipped with their reason:
idempotent producers, which this binding does not wrap, and
`error.bad_offset_is_typed`, which needs a trimmed log that a client has no way
to produce.

## Building

The crate lives outside the workspace, like `felix-python` and the crates under
`demos/`: a Node addon is a `cdylib` whose Node symbols the host process
resolves at load time, which is correct for an addon and fatal for a test
binary linked by `cargo test --workspace`.

```bash
task ts:check     # fmt, clippy, build — what CI runs
task ts:build     # napi build --release (needs `npm i -g @napi-rs/cli@2`)
```

Without the napi CLI you can still load the addon, because `napi build` is
mostly a rename: build the `cdylib` and point Node at it directly.

```bash
cargo build --release
cp ../../target/release/libfelix_typescript.dylib ./felix.node   # .so on Linux
node -e "console.log(Object.keys(require('./felix.node')))"
```
