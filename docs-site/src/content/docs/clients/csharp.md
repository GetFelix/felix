---
title: "C# Client"
description: "Using the Felix C# client: supported platforms, connecting, publishing, subscribing, typed errors, cancellation and threading."
---

`Felix.Client` is the C# client, in `sdks/dotnet`. It binds the [C ABI](/clients/c/)
over the Rust client with P/Invoke, so reconnection, redirect-following and error
classes are the Rust client's. Every call is a `Task`; no thread waits on a pending
publish or read.

It covers connect, publish (single and batch) and subscribe to one shard. Consumer
groups, caches, commits, producers and multi-shard subscribe are bound in later
steps, and it is not on NuGet yet.

## Supported platforms

| Platform | Architectures |
| --- | --- |
| Linux | x64, arm64 |
| macOS | x64, arm64 |
| Windows | x64, arm64 |

It targets net8.0 and net10.0. CI builds it and runs the unit tests, including a load
of the native library and a call into it, natively on each row above. The scenarios
against a broker run on Linux x64 only.

The native libraries will ship inside the NuGet package in a later release. Until then,
build `felix-capi` yourself and set `FELIX_NATIVE_LIB`, as below.

## Building

The SDK needs `libfelix` (`libfelix.so`, `libfelix.dylib` or `felix.dll`), built from
the repository:

```bash
cargo build --release -p felix-capi
export FELIX_NATIVE_LIB="$PWD/target/release/libfelix.so"
dotnet test sdks/dotnet/Felix.sln
```

The library is found at `FELIX_NATIVE_LIB`, next to `Felix.Client.dll`, or under
`runtimes/<rid>/native/` (for example `runtimes/linux-x64/native/`).

## Connect, publish, subscribe

```csharp
using Felix;

await using var client = await FelixClient.ConnectAsync(new FelixClientOptions
{
    Addrs = ["127.0.0.1:5000"],
    TenantId = "t1",
    Token = "my-token",
});

// Subscribe first, so the publish below is not missed.
await using var sub = await client.SubscribeAsync(
    "t1", "default", "events", shard: 0, StartPosition.Latest);

long? offset = await client.PublishAsync(
    "t1", "default", "events", "hello"u8.ToArray());

await foreach (var ev in sub)
{
    Console.WriteLine($"{ev.Offset}: {ev.Payload.Length} bytes");
    break;
}
```

`PublishAsync` returns the record's log offset, or null when the stream has no log,
the ack mode is `AckMode.None`, or the broker acknowledged before writing. A `key`
routes the record: records sharing a key share a shard and stay in order.
`PublishBatchAsync` sends several records to one shard as a unit and returns each
record's offset.

`ReadAsync(timeout)` returns the next event, or null if the timeout passes; the
subscription stays usable and nothing is lost. Enumerating it with `await foreach`
reads until the broker ends the stream, and `CloseReason` then says whether the stream
ended or was deleted. Only one read may be pending at a time. A `FelixEvent` holds a
copy of the payload, so it stays valid after the next read. `StartOffset` is where
the subscription began.

## Errors

Failures are `FelixException`s whose `Status` is the C ABI's status code (`Auth`,
`NotFound`, `Connection`, `ShardUnavailable`, `Overloaded`, `OutcomeUnknown` and so on).
`IsRetryable` is true for `Connection`, `ShardUnavailable` and `Overloaded`, where
nothing was applied. `OutcomeUnknown` means a write may have landed, so resend only if
it is idempotent. Cancellation is an `OperationCanceledException`, not a
`FelixException`. See [Status codes](/clients/c/#status-codes).

## Cancellation

Each call takes a `CancellationToken`, which cancels the native call. A cancelled
connect or subscribe hands out nothing, even if the native call finished as you cancelled. A publish cancelled after it may have been
sent fails with `OutcomeUnknown`, because it may have landed.

## Threading

A client is safe to use from several threads. Publishes in flight together may reach the
broker in any order; to keep records in order, await each one before the next.
Calls on a disposed client throw `ObjectDisposedException`. Disposing the client cancels its pending calls, which fail with `OperationCanceledException`, and `DisposeAsync` frees it off the
calling thread because the last handle can wait a few seconds for them. Subscriptions
made from a client keep working until they are disposed.
