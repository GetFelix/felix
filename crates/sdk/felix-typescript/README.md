# felix-client for Node.js

[Felix](https://github.com/GetFelix/felix) is a broker that serves streams, work queues and a cache from one replicated log, over QUIC. This is the Node.js and TypeScript client. It publishes to and reads streams, runs consumer groups, uses the cache and counters, and follows a cluster as brokers fail and shards move. It is a binding over the Rust client, so reconnection, redirects, retry classification and offsets behave as they do there.

## Install

```bash
npm install felix-client
```

## Quick start

Connect, subscribe, publish one record and read it back. Every call returns a `Promise`:

```ts
import { Client } from "felix-client";

const token = process.env.FELIX_TOKEN;
if (!token) throw new Error("set FELIX_TOKEN");

const client = await Client.connect("127.0.0.1:5000", "t1", { token });
try {
  // Subscribe before publishing: a subscription starts at the stream's tail.
  const events = await client.subscribe("t1", "default", "events");
  try {
    await client.publish("t1", "default", "events", Buffer.from("hello"));
    const event = await events.nextEvent();
    if (event !== null) console.log(event.payload.toString(), event.offset);
  } finally {
    await events.close();
  }
} finally {
  client.close();
}
```

To run it you need a running broker, a token for tenant `t1`, and the stream `t1/default/events`. The [quick start](https://docs.getfelix.dev/getting-started/quickstart/) shows how to start a broker, [Auth and TLS](https://docs.getfelix.dev/clients/auth-and-tls/) covers tokens, and [`felixctl stream create`](https://docs.getfelix.dev/getting-started/felixctl/) makes the stream.

Every handle has an idempotent `close()` and implements `Symbol.asyncDispose`, so on Node 24 and newer `await using events = await client.subscribe(...)` releases it for you.

## What it covers

- Publish and subscribe, with a log offset on every event of a durable stream
- Sharded streams: routing keys, named shards, and subscribing to every shard
- Consumer groups: poll, acknowledge, retry later, dead letters
- Cache, counters and cache watches
- Atomic commits, conditional writes and head trims
- Record headers, and who published a record and when
- Idempotent producers, including handing batches over without waiting
- Lease tokens, and cluster inspection (`inspectShard`, `listSubscriptions`, `topology`)
- Token refresh, cluster discovery, failover and following a shard that moves

Headers travel next to the payload and come back on the event:

```ts
await client.publishWithHeaders("t1", "default", "orders",
  { key: Buffer.from("order-42"), headers: [{ name: "trace-id", value: Buffer.from("7f3a") }] },
  Buffer.from("{}"));
// later: event.headers?.headers[0].name === "trace-id"
```

A publish without a key puts every record on shard 0. `atLeastOnce` publishing resends after an ambiguous failure and can write a record twice; an idempotent producer (`client.producer()`) resends without the duplicate.

## Configuration

`Client.connect(addrs, tenantId, options)` takes the addresses and the tenant as arguments, then an options object:

| Argument or option | Meaning |
| --- | --- |
| `addrs` (argument) | One `host:port` or an array. Any reachable broker is enough. |
| `tenantId` (argument) | The tenant to authenticate as. |
| `token` | A Felix token. |
| `tokenProvider` | A function that returns a fresh token, or a promise of one. It receives an `AbortSignal`. `token` is then optional. |
| `caFile` | A PEM file of CAs to trust. The system store when omitted. |
| `serverName` | The name the broker's certificate carries. Default `localhost`. |
| `clientCertFile`, `clientKeyFile` | The client certificate and its key, as PEM files. |
| `reconnectAttempts`, `reconnectDeadline`, `reconnectBackoff`, `reconnectMaxBackoff` | Retry policy. Times are in milliseconds. |
| `publishers`, `timestamps` | Fill `Event.publisher` and `Event.timestampMicros`. Off by default. |
| `clientSubQueuePolicy`, `clientSubQueueCapacity`, `brokerSubQueueCapacity` | What a subscription queue does when full, and how large it is. |
| `publishConnPool`, `maxFrameBytes`, `connectTimeout`, and the other transport options | Connection pools, QUIC windows, frame and timeout limits. The full list is on the [TypeScript page](https://docs.getfelix.dev/clients/typescript/#tuning-the-client). |

TLS is always on, and there is no switch to skip verification. A development broker with a self-signed certificate needs its CA file. A broker started with `FELIX_TLS_CLIENT_CA` also requires a client certificate. Give the certificate and its key as two PEM files, both or neither.

A token lasts 15 minutes by default. A process that runs longer passes `tokenProvider` so the client can fetch a new one. [Auth and TLS](https://docs.getfelix.dev/clients/auth-and-tls/) lists the same options for every client.

## Errors

Every error is a `FelixError`, with `kind`, `code`, `retry`, `detail` and `retryable`:

```ts
import { AuthError, FelixError, OutcomeUnknownError } from "felix-client";

try {
  await client.publish("t1", "default", "events", Buffer.from("hello"));
} catch (err) {
  if (err instanceof OutcomeUnknownError) {
    // The record may have been written. Reconcile before sending it again.
  } else if (err instanceof AuthError) {
    // The token was refused. Retrying will not help.
  } else if (err instanceof FelixError && err.retryable) {
    // Nothing was written. Send it again.
  } else {
    throw err;
  }
}
```

The other classes are `ConnectionError`, `ShardUnavailableError`, `OverloadedError`, `NotFoundError`, `CursorError`, `InvalidArgumentError`, `CommitError` (with `NotOnOwningShardError` and `EventCountError`), `TrimError` and `PublishRefusedError`. The client already retries what is safe to retry. Do not match on the message text. [Errors across clients](https://docs.getfelix.dev/clients/errors/) maps every code to the type each client raises.

## Supported platforms

Node 18 or newer. The native binary comes in a per-platform package that npm installs on its own, so nothing is compiled at install time. Linux (x86-64 and arm64, glibc 2.28 or newer), macOS (Intel and Apple silicon) and Windows (x86-64). Alpine and other musl systems are not supported.

## What is not wrapped

The Rust client has two things this binding leaves out on purpose:

- `publish_json` and `publish_batch_json`. Publish `Buffer.from(JSON.stringify(value))`.
- A custom `quinn` transport (`connect_with_policy`, `connect_any_with_transport`). It takes a Rust transport object a Node program cannot build.

## Documentation

- [Node.js and TypeScript client guide](https://docs.getfelix.dev/clients/typescript/)
- [Choosing a client](https://docs.getfelix.dev/clients/overview/)
- [Delivery semantics](https://docs.getfelix.dev/architecture/semantics/)

## License

Apache-2.0.
