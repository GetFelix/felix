# Felix.Client

The C# client for Felix. It binds the C ABI in `crates/sdk/felix-capi`, which wraps the Rust
client, so reconnection, redirects and error classes are the Rust client's. Targets net8.0 and
net10.0 on Linux, macOS and Windows, each on x64 and arm64; CI tests every one of those natively
(the broker scenarios on Linux x64 only). The native libraries will ship inside the NuGet package in a later release. Until then, build `felix-capi` and set `FELIX_NATIVE_LIB`.

This step covers connect, publish (single and batch) and subscribe to one shard. Not packaged
or released yet. See [the C# page](../../docs-site/src/content/docs/clients/csharp.md).

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
