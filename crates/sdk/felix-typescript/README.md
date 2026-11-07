# felix-client for Node.js

The Node.js and TypeScript client for [Felix](https://github.com/GetFelix/felix). It publishes to and reads from streams, runs consumer groups, uses the cache and counters, and follows a cluster as brokers fail and shards move. It is a binding over the Rust client, so reconnection, redirects, retry classification and offsets behave as they do there.

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

The stream has to exist first; create it with [`felixctl stream create`](https://docs.getfelix.dev/getting-started/felixctl/).

Every handle has an idempotent `close()` and implements `Symbol.asyncDispose`, so on Node 24 and newer `await using events = await client.subscribe(...)` releases it for you.

## What it covers

- Publish and subscribe, with a log offset on every event of a durable stream
- Sharded streams: routing keys, named shards, and subscribing to every shard
- Consumer groups: poll, acknowledge, retry later, dead letters
- Cache, counters and cache watches
- Atomic commits, conditional writes and head trims
- Idempotent producers
- Token refresh, cluster discovery, failover and following a shard that moves

Record headers and client certificates (mutual TLS) are Rust only. A publish without a key puts every record on shard 0. `atLeastOnce` publishing resends after an ambiguous failure and can write a record twice; an idempotent producer (`client.producer()`) resends without the duplicate.

## Configuration

`Client.connect(addrs, tenantId, options)` takes the addresses and the tenant, then these options:

| Option | Meaning |
| --- | --- |
| `addrs` | One `host:port` or an array. Any reachable broker is enough. |
| `tenantId` | The tenant to authenticate as. |
| `token` | A Felix token. |
| `tokenProvider` | A function that returns a fresh token, or a promise of one. It receives an `AbortSignal`. `token` is then optional. |
| `caFile` | A PEM file of CAs to trust. The system store when omitted. |
| `serverName` | The name the broker's certificate carries. Default `localhost`. |
| `reconnectAttempts`, `reconnectDeadline`, `reconnectBackoff`, `reconnectMaxBackoff` | Retry policy. Times are in milliseconds. |

TLS is always on, and there is no switch to skip verification. A development broker with a self-signed certificate needs its CA file.

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

The other classes are `ConnectionError`, `ShardUnavailableError`, `OverloadedError`, `NotFoundError`, `CursorError` and `InvalidArgumentError`. The client already retries what is safe to retry. Do not match on the message text. [Errors across clients](https://docs.getfelix.dev/clients/errors/) maps every code to the type each client raises.

## Supported platforms

Node 18 or newer. The native binary comes in a per-platform package that npm installs on its own, so nothing is compiled at install time. Linux (x86-64 and arm64, glibc 2.28 or newer), macOS (Intel and Apple silicon) and Windows (x86-64). Alpine and other musl systems are not supported.

## Documentation

- [Node.js and TypeScript client guide](https://docs.getfelix.dev/clients/typescript/)
- [Choosing a client](https://docs.getfelix.dev/clients/overview/)
- [Delivery semantics](https://docs.getfelix.dev/architecture/semantics/)

## License

Apache-2.0.
