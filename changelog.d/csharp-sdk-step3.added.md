- C# SDK (`sdks/dotnet`): caches, counters and cache watches. `FelixClient.Cache` gives a `FelixCache` with
  put (with a TTL), get, versioned get, delete, `PutIfAbsentAsync`, `PutIfVersionAsync`, `DeleteIfVersionAsync`
  and counter add and get. `WatchKeyAsync`, `WatchPrefixAsync` and `WatchShardedAsync` open a
  `FelixCacheWatch`, an `IAsyncEnumerable<WatchItem>` that surfaces lag, shard moves and
  state-complete as items, with `RetainedCount`, `fromOffset` and `ResumeOffsets()` for resuming.
  Fixture tests tag the cache, counter and watch conformance scenarios.
