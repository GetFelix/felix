- A cluster client's cache watch no longer ends when the broker serving it dies. It is reopened on
  whichever broker owns its shard now, from the offset after the last change it delivered, under the
  client's `ReconnectPolicy`, and the Python, TypeScript, C# and C watches get the same. If no broker
  takes it in time, `recv` returns `None` and `ClusterCacheWatch::error` says why. A bare
  `CacheWatch` reports a broken connection through the new `lost()` (GetFelix/felixd#68).
- A retained cluster cache watch that moves or loses its broker during its state phase is reopened
  retained, so its state phase starts over on the new owner instead of ending with values missing.
  A sharded retained watch no longer sends `StateComplete` before that shard's state has all
  arrived. Values already handed out may arrive again inside the restarted state phase;
  `ClusterCacheWatch::in_state_phase` says when it is over.
- `ShardedCacheWatchItem::ShardClosed` now carries an `error` saying why the shard could not be
  followed. Cache watches expose their reconnect count and end cause in every binding: `reconnects`
  and `error` in Python and TypeScript (plus `CacheShardClosed.error` and `shardClosedError`),
  `felix_cache_watch_reconnects` and the closing read's message in C, and `Reconnects`, `Error` and
  `WatchItem.Error` in C#.
- A cluster client no longer keeps routing cache requests to a shard owner it cannot connect to. A
  failed connect drops the cached route, so the next request asks who owns the shard instead of
  waiting out a connect timeout on the dead broker every time (GetFelix/felixd#68).
