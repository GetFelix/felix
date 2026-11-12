# Felix.Client

[Felix](https://github.com/GetFelix/felix) is a broker that serves streams, work queues and a cache from one replicated log, over QUIC. This is the C# client. It publishes to and reads streams, runs consumer groups, uses the cache and counters, and follows a cluster as brokers fail and shards move. It is a binding over the Felix C ABI, which wraps the Rust client, so reconnection, redirects, retry classification and offsets behave as they do there.

## Install

```bash
dotnet add package Felix.Client
```

The package carries the native library, so there is nothing else to install.

## Quick start

Connect, subscribe, publish one record and read it back:

```csharp
using Felix;

var token = Environment.GetEnvironmentVariable("FELIX_TOKEN")
    ?? throw new InvalidOperationException("set FELIX_TOKEN");

await using var client = await FelixClient.ConnectAsync(new FelixClientOptions
{
    Addrs = ["127.0.0.1:5000"],
    TenantId = "t1",
    Token = token,
});

// Subscribe before publishing: StartPosition.Latest starts at the stream's tail.
await using var events = await client.SubscribeAsync(
    "t1", "default", "events", shard: 0, StartPosition.Latest);
await client.PublishAsync("t1", "default", "events", "hello"u8.ToArray());

FelixEvent? received = await events.ReadAsync(TimeSpan.FromSeconds(5));
if (received is null)
{
    throw new TimeoutException("no event within 5 seconds");
}
Console.WriteLine($"{received.Offset}: {System.Text.Encoding.UTF8.GetString(received.Payload.Span)}");
```

To run it you need a running broker, a token for tenant `t1`, and the stream `t1/default/events`. The [quick start](https://docs.getfelix.dev/getting-started/quickstart/) shows how to start a broker, [Auth and TLS](https://docs.getfelix.dev/clients/auth-and-tls/) covers tokens, and [`felixctl stream create`](https://docs.getfelix.dev/getting-started/felixctl/) makes the stream. A subscription is also an `IAsyncEnumerable<FelixEvent>`, so `await foreach (var ev in events)` reads until the broker ends the stream.

## What it covers

- Publish and subscribe, with a log offset on every event of a durable stream
- Sharded streams: routing keys, named shards, and subscribing to every shard
- Consumer groups: poll, acknowledge, retry later, dead letters
- Cache, counters and cache watches
- Atomic commits, conditional writes and head trims
- Idempotent producers, and hand-offs that queue batches in order without waiting for each answer
- Inspecting the cluster: what a broker supports, shard owners and routing, one shard's state, and the subscriptions a broker serves
- Subscription counters: `Moves`, `Reconnects`, `StreamDeleted` and `QueueCapacity`
- Record headers, page reads by offset range, lease-token writes and per-principal identities
- Token refresh, cluster discovery, failover and following a shard that moves

A publish without a `key` puts every record on shard 0. A record's own key and headers (`PublishWithHeadersAsync`) do not route.

## Configuration

`FelixClientOptions` has these properties:

| Property | Meaning |
| --- | --- |
| `Addrs` | Broker addresses as `host:port`. Any reachable one is enough. Required. |
| `TenantId` | The tenant to authenticate as. Required. |
| `Token` | A Felix token. |
| `TokenProvider` | A `Func<CancellationToken, ValueTask<string>>` that returns a fresh token. `Token` is then optional. |
| `CaFile` | A PEM file of CAs to trust. The system store when null. |
| `ServerName` | The name the broker's certificate carries. Default `localhost`. |
| `ClientCertFile`, `ClientKeyFile` | The client certificate and its key, as PEM files. |
| `DisableRecordHeaders` | Stop offering record headers to brokers. Off by default. |
| `ReconnectAttempts`, `ReconnectDeadline`, `ReconnectBackoff`, `ReconnectMaxBackoff` | Retry policy. `ReconnectAttempts` is an `int`; the others are `TimeSpan`. |
| `ClientSubQueuePolicy`, `ClientSubQueueCapacity`, `BrokerSubQueueCapacity` | What a subscription's queue does when full, and how many events and batches it holds. Null is the default. |
| `PublishConnPool`, `PublishSharding`, `MaxFrameBytes`, `AckOnCommit`, `ConnectTimeout`, `FinishTimeout` and the other pool, window and queue sizes | Transport tuning. Null is the default. The [C# client guide](https://docs.getfelix.dev/clients/csharp/#queue-and-transport-options) lists them all. |

TLS is always on, and there is no switch to skip verification. A development broker with a self-signed certificate needs its CA file. A broker started with `FELIX_TLS_CLIENT_CA` also requires a client certificate. Give the certificate and its key as two PEM files, both or neither.

A token lasts 15 minutes by default. A process that runs longer sets `TokenProvider` so the client can fetch a new one. [Auth and TLS](https://docs.getfelix.dev/clients/auth-and-tls/) lists the same options for every client.

## Errors

A failed call throws `FelixException`. Read its `Status`:

```csharp
try
{
    await client.PublishAsync("t1", "default", "events", "hello"u8.ToArray());
}
catch (FelixException e) when (e.OutcomeUnknown)
{
    // The record may have been written. Reconcile before sending it again.
}
catch (FelixException e) when (e.Status == FelixStatus.Auth)
{
    // The token was refused. Retrying will not help.
}
catch (FelixException e) when (e.IsRetryable)
{
    // Nothing was written. Send it again.
}
```

`IsRetryable` is true for `Connection`, `ShardUnavailable` and `Overloaded`. A cancelled call throws `OperationCanceledException`. The client already retries what is safe to retry. Do not match on the message text. [Errors across clients](https://docs.getfelix.dev/clients/errors/) maps every code to the type each client raises.

## Supported platforms

net8.0 and net10.0 on Linux, macOS and Windows, each on x64 and arm64. The package carries the native library for all six, built for glibc, so Alpine and other musl systems need a library built by hand (see [Building the native library yourself](https://docs.getfelix.dev/clients/csharp/#building-the-native-library-yourself)). The broker scenarios run in CI on Linux x64 only.

## Documentation

- [C# client guide](https://docs.getfelix.dev/clients/csharp/)
- [Choosing a client](https://docs.getfelix.dev/clients/overview/)
- [Delivery semantics](https://docs.getfelix.dev/architecture/semantics/)

## License

Apache-2.0.
