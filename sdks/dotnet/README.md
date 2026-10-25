# Felix.Client

The C# client for Felix. It binds the C ABI in `crates/sdk/felix-capi`, which wraps the Rust
client, so reconnection, redirects and error classes are the Rust client's. Targets net8.0 and
net10.0 on Linux, macOS and Windows, each on x64 and arm64; CI tests every one of those natively
(the broker scenarios on Linux x64 only). The native libraries will ship inside the NuGet package in a later release. Until then, build `felix-capi` and set `FELIX_NATIVE_LIB`.

It binds every function in `felix.h` except the token provider, which follows: connect, publish
(single, batch, keyed records and named-shard), subscribe to one shard or every shard and resume
(`SubscribeShardedAsync`, `ResubscribeShardedAsync`), consumer groups (on a shard and across
every shard, with the stream-wide admin calls), caches, counters and cache watches (single and
sharded, retained, resumable), atomic commits with state reads and conditional publishes
(`CommitAsync`, `CommitIfAsync`, `PublishIfAsync`, `GetStateAsync`), idempotent producers
(`CreateProducerAsync`), endpoints, offset lookup by time, and the publisher and timestamp
metadata. `python3 scripts/check_csharp_bindings.py` fails if an export of `felix.h` is not bound.
Not packaged or released yet. See [the C# page](../../docs-site/src/content/docs/clients/csharp.md).

```csharp
await using var client = await FelixClient.ConnectAsync(new FelixClientOptions
{
    Addrs = ["127.0.0.1:5000"],
    TenantId = "t1",
    Token = "my-token",
});
await using var sub = await client.SubscribeAsync("t1", "default", "events", 0, StartPosition.Latest);
long? offset = await client.PublishAsync("t1", "default", "events", "hello"u8.ToArray());
await foreach (var ev in sub) { Console.WriteLine(ev.Offset); break; }
```

## Build and test

```bash
cargo build --release -p felix-capi
export FELIX_NATIVE_LIB="$PWD/target/release/libfelix.so"   # .dylib on macOS, felix.dll on Windows
dotnet test sdks/dotnet/Felix.sln
```

The native library is found at `FELIX_NATIVE_LIB`, next to `Felix.Client.dll`, or under
`runtimes/<rid>/native/`. Scenario tests against a broker are skipped unless `FELIX_FIXTURE` names
the file written by `felix-cluster client-fixture --out FILE --ca-file CA.PEM`. The version comes
from the workspace `Cargo.toml`; pass `-p:Version=...` to override it.

Tests tag the conformance scenarios they cover with `[Scenario("id")]`. Set
`FELIX_CONFORMANCE_RESULTS=FILE` to write the results document, then check it with
`cargo run -p felix-conformance -- verify FILE`.
