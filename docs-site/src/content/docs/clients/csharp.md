---
title: "C# Client"
description: "Using the Felix C# client: supported platforms, connecting, publishing, subscribing, consumer groups, publisher and timestamp metadata, typed errors, cancellation and threading."
---

`Felix.Client` is the C# client, in `sdks/dotnet`. It binds the [C ABI](/clients/c/)
over the Rust client with P/Invoke, so reconnection, redirect-following and error
classes are the Rust client's. Every call is a `Task`; no thread waits on a pending
publish or read.

It covers connect, publish (single and batch), subscribe to one shard, consumer groups
(on a shard and across every shard) and the publisher and timestamp fields. Caches,
commits, producers and multi-shard subscribe are bound in later steps, and it is not on
NuGet yet.

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

## Consumer groups

`client.JoinGroup(tenant, ns, stream, shard, group, consumer)` returns a `FelixGroup` for one
shard. It sends nothing and holds no native state; it is a name for the group's calls.

```csharp
var admin = adminClient.JoinGroup("t1", "default", "orders", shard: 0, "billing");
await admin.CreateAsync(StartPosition.Earliest);   // Moved is false if it was already there

var group = client.JoinGroup("t1", "default", "orders", shard: 0, "billing", consumer: "worker-1");
foreach (var record in await group.PollAsync(maxRecords: 32, wait: TimeSpan.FromSeconds(5)))
{
    try
    {
        Handle(record.Payload.Span);
        await group.AckAsync(record);
    }
    catch
    {
        await group.NackAsync(record, delay: TimeSpan.FromSeconds(30));
    }
}
```

- `PollAsync` returns up to `maxRecords` as `GroupRecord`s with the offset, `Attempts` (1 is a
  first delivery), `SkippedBefore`, a copy of the payload and the shard. An empty array is an
  answer, not an error. `visibility` overrides the broker's claim timeout, `consumer` names the
  member, and `reclaim` takes back what that member held before a restart (it needs a consumer
  name). A zero count, or `reclaim` without a consumer, is an `InvalidArgument` failure. A poll
  cancelled while it waits fails with `OperationCanceledException`, and what it claimed comes back
  after the visibility timeout.
- `AckAsync`, `NackAsync` (with a delay), `ExtendAsync` (returns how long the claim now stands),
  `DeadLetterAsync`, `DiscardAsync` and `RedriveAsync` settle a record. Nack and extend name the
  delivery by the record's `Attempts`, so they are refused once the claim has lapsed or the
  record was handed out again. Settles are writes: cancelled after they may have been sent they
  fail with `OutcomeUnknown`, and settling again is refused or harmless.
- `DeadLettersAsync` lists the shard's dead-letter offsets and `DescribeAsync` returns a
  `GroupInfo` (`Committed`, `Tail`, `InFlight`, `Owed`, `DeadLetters`; `InFlight` and `Owed` are
  leader memory and restart from zero when the leader changes). `CreateAsync`, `SeekAsync` and
  `DeleteAsync` manage the group; `DeleteAsync` says whether it existed.

Creating, seeking and deleting a group, and discarding or redriving a dead letter, need the
`group.manage` permission; polling, settling, listing and describing need only consumer access.

### All shards

`client.GetShardCountAsync` returns a stream's shard count, and zero for a stream the broker does
not know, not one. `client.OpenShardedGroupAsync` opens a `FelixShardedGroup` over every shard; it
creates nothing, so create the group first with the stream-wide calls
`CreateStreamGroupAsync`, `SeekStreamGroupAsync`, `DescribeStreamGroupAsync` and
`DeleteStreamGroupAsync`. They run one request per shard and return a `GroupPosition` or
`GroupInfo` per shard, indexed by shard. A failure partway leaves the earlier shards changed; every
call is safe to repeat. An offset start is refused for a stream with more than one shard, since
an offset means something different on each; use `StartPosition.Earliest` or `Latest`.

`FelixShardedGroup.PollAsync` visits the shards in turn and returns the first non-empty batch.
Order holds within a shard and nowhere across shards, and a shard that cannot be polled fails the
whole poll. Settle a record with the methods on the sharded group, which route by
`GroupRecord.Shard`. Disposing the group ends a pending poll as cancelled.

## Publisher and timestamp

Brokers send a record's publisher and append time only when asked:

```csharp
var client = await FelixClient.ConnectAsync(new FelixClientOptions
{
    Addrs = ["127.0.0.1:5000"], TenantId = "t1", Token = "my-token",
    Publishers = true, Timestamps = true,
});
```

`FelixEvent` and `GroupRecord` then carry `Publisher` (the principal), and `TimestampMicros` with
`Timestamp` as a `DateTimeOffset?`, by the clock of the broker that led the shard. Publishers add the
principal to every event batch and timestamps add eight bytes per event. A broker that does not
negotiate either, or an in-memory stream that stores no time, leaves them null; that is never an
error.

## Conformance

Fixture tests carry `[Scenario("id")]` for the scenarios of the client conformance catalogue
they cover. With `FELIX_CONFORMANCE_RESULTS` set, the test run writes the results document
`felix-conformance verify` reads. CI runs that verdict for C#, but it does not block yet: the SDK
still lacks the scenarios for caches, commits and multi-shard subscribe.

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
