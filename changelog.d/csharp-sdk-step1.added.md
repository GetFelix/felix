- C# SDK, `Felix.Client` (`sdks/dotnet`): connect, publish (single and batch) and subscribe to a
  shard, bound to the C ABI over the Rust client. Targets net8.0 and net10.0 on Linux, macOS and
  Windows (x64 and arm64). Failures are typed `FelixException`s with the C ABI's retry classes, and
  cancellation tokens cancel the native call. A `dotnet` CI job builds and tests it on all three
  platforms and runs the fixture scenarios on Linux. It is not packaged or released yet.
