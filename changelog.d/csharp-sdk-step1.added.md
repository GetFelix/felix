- C# SDK, `Felix.Client` (`sdks/dotnet`): connect, publish (single and batch) and subscribe to a
  shard, bound to the C ABI over the Rust client. Targets net8.0 and net10.0 on Linux, macOS and
  Windows, each on x64 and arm64. The native libraries will ship in the NuGet package in a later
  release; until then, build `felix-capi` and set `FELIX_NATIVE_LIB`. Failures are typed `FelixException`s with the C ABI's retry classes, and
  cancellation tokens cancel the native call. A `dotnet` CI job builds and tests it natively on all
  six platform and architecture pairs and runs the fixture scenarios on Linux x64. It is not packaged or released yet.
