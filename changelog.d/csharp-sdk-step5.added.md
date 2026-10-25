- C# SDK (`sdks/dotnet`): parity with the C ABI, except the token provider. `SubscribeShardedAsync`
  and `ResubscribeShardedAsync` open every shard as a `FelixShardedSubscription` that reports
  records and shard changes (`ShardItem`) and gives `Positions()` to resume from; `PublishOnShardAsync`,
  `PublishRecordsAsync` (a `PublishResult` per record), `OffsetForTimeAsync` and `GetEndpointsAsync`
  bind the remaining exports. `scripts/check_csharp_bindings.py` fails CI when an export of
  `felix.h` is unbound and not allowlisted with a reason, and the C# conformance verdict now blocks.
