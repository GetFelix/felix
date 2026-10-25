---
title: "C# Client"
description: "Using the Felix C# client: supported platforms, connecting, publishing, subscribing, consumer groups, caches, counters and watches, publisher and timestamp metadata, typed errors, cancellation and threading."
---

`Felix.Client` is the C# client, in `sdks/dotnet`. It binds the [C ABI](/clients/c/)
over the Rust client with P/Invoke, so reconnection, redirect-following and error
classes are the Rust client's. Every call is a `Task`; no thread waits on a pending
publish or read.

It covers connect, publish (single and batch), subscribe to one shard, consumer groups
(on a shard and across every shard), caches, counters and cache watches, and the publisher
and timestamp fields. Commits, producers and multi-shard subscribe are bound in later
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

## Getting and refreshing a token

The SDK does not call the control plane. Use `HttpClient`; the refresh token
rotates on every use, so keep the newest one:

```csharp
using System.Net.Http.Json;
using System.Text.Json.Serialization;

var http = new HttpClient { BaseAddress = new Uri("https://controlplane:8443/v1/tenants/t1/") };

var exchange = new HttpRequestMessage(HttpMethod.Post, "token/exchange")
{
    Content = JsonContent.Create(new
    {
        requested = new[] { "stream.publish", "stream.subscribe" },
        resources = new[] { "stream:t1/default/*" },
    }),
};
exchange.Headers.Authorization = new("Bearer", oidcToken);
var minted = await (await http.SendAsync(exchange)).EnsureSuccessStatusCode()
    .Content.ReadFromJsonAsync<Minted>();

// Before minted.ExpiresIn seconds pass (say after two thirds of it):
var refreshed = await http.PostAsJsonAsync("token/refresh",
    new { refresh_token = minted!.RefreshToken });
minted = await refreshed.EnsureSuccessStatusCode().Content.ReadFromJsonAsync<Minted>();

// Type declarations go after top-level statements.
record Minted(
    [property: JsonPropertyName("felix_token")] string FelixToken,
    [property: JsonPropertyName("expires_in")] int ExpiresIn,
    [property: JsonPropertyName("refresh_token")] string RefreshToken);
```

`FelixClientOptions.Token` cannot change on a live client. After a refresh,
connect a new `FelixClient` with `minted.FelixToken`, move your work to it, and
dispose the old one. A refresh that answers `403` means the refresh token
expired or was revoked: run the exchange again.

## Connect, publish, subscribe

`Token` is a Felix token; [Getting a token](/clients/overview/#getting-a-token)
covers where it comes from. `FelixClientOptions.Token` is a fixed string and the
client cannot refresh it, so dispose the client and connect a new one with a
fresh token before it expires. Streams opened after expiry, including the ones a
reconnect opens, are refused.

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

## Caches, counters and watches

`client.Cache(tenant, ns, cache)` returns a `FelixCache`. It sends nothing and holds no native
state; it is a name for the cache's calls. Keys are UTF-8 strings and values are bytes.

```csharp
var cache = client.Cache("t1", "default", "sessions");

await cache.PutAsync("user:1", "alice"u8.ToArray(), ttl: TimeSpan.FromMinutes(5));
ReadOnlyMemory<byte>? value = await cache.GetAsync("user:1");     // null if missing or expired
ReadOnlyMemory<byte>? removed = await cache.DeleteAsync("user:1"); // the value it held, or null

long total = await cache.CounterAddAsync("hits", 1);
long? now = await cache.CounterGetAsync("hits");                   // null if never written, not zero
```

- A missing or expired key is `null`, not an error, and an empty value is a value. `ttl` is rounded up
  to whole milliseconds; null is for ever, and a zero or negative TTL is an `ArgumentOutOfRangeException`.
- `GetVersionedAsync` returns a `VersionedValue` whose `Version` a conditional write can name.
  `PutIfAbsentAsync`, `PutIfVersionAsync` and `DeleteIfVersionAsync` return a `ConditionalResult`: a
  condition that does not hold is `Applied` false with the current `Version` when the broker reports
  it, not an exception.
- Puts are safe to repeat. A write cancelled after it may have been sent fails with `OutcomeUnknown`.
  A counter add is not idempotent, so after `OutcomeUnknown` read the counter before adding again.
  A counter saturates at `long.MaxValue` and `long.MinValue` instead of wrapping.
- Strings handed to the library (tenant, namespace, stream, group, cache, key, prefix, token,
  addresses) must not contain a NUL character: C would end the string there, so every call throws an
  `ArgumentException` rather than act on a shorter name.

### Watches

`WatchKeyAsync` and `WatchPrefixAsync` open a `FelixCacheWatch` on one shard (a key is routed to its own
shard; a prefix reads one, so use it on a single-shard cache). `WatchShardedAsync` watches a prefix on
every shard and fails rather than cover fewer shards than the cache has. `retained: true` delivers the
stored entries first as changes; a sharded retained watch then yields `StateComplete`, and
`RetainedCount` says how many entries to expect.

```csharp
await using var watch = await cache.WatchShardedAsync("user:");
await foreach (var item in watch)
{
    switch (item.Kind)
    {
        case WatchItemKind.Change when item.IsDelete:
            Remove(item.Key!);
            break;
        case WatchItemKind.Change:
            Store(item.Key!, item.Value!.Value, item.ExpiresAt);
            break;
        case WatchItemKind.Lagged:
            // The broker ended this feed; re-watch from item.ResumeFrom.
            break;
    }
}
```

- A `WatchItem` is a copy. A deleted key has a null `Value`, which an empty value is not. `Shard` is
  null on a single watch. A filtered watch's offsets are sparse because other keys' changes consume
  them, so a gap is not a drop; `Lagged` is.
- Enumerating yields every item, `Lagged` included, and ends when the feed does: after a lag on a single
  watch, and once every shard of a sharded watch has ended. A lag on a sharded watch ends only
  `Shard`; `ShardClosed` reports a shard whose feed ended, and `ShardMoved` that the watch is following
  a shard to another broker by itself.
- `ReadAsync(timeout)` returns null on a timeout and loses nothing when it times out or is cancelled.
  One read may be pending at a time. Disposing the watch ends a pending read as cancelled.
- To resume, pass `fromOffset` on a single watch: the first offset not yet seen, which is the last handled
  change's `Offset` plus one, or a `Lagged` item's `ResumeFrom`. For a sharded watch, pass
  `resume: watch.ResumeOffsets()`, which lists every shard (in ascending order) at an offset never ahead of
  what reads have handed out. `ResumeOffsets` fails with `InvalidArgument` on a single watch, and a
  retained watch takes no offsets.

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
still lacks the scenarios for commits, producers and multi-shard subscribe.

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
