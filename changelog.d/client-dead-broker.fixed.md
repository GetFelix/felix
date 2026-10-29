- A cluster client's cache watch no longer ends when the broker serving it dies. It is reopened on
  whichever broker owns its shard now, from the offset after the last change it delivered, under the
  client's `ReconnectPolicy`, and the Python, TypeScript, C# and C watches get the same. If no broker
  takes it in time, `recv` returns `None` and `ClusterCacheWatch::error` says why. A bare
  `CacheWatch` reports a broken connection through the new `lost()` (GetFelix/felixd#68).
- A cluster client no longer keeps routing cache requests to a shard owner it cannot connect to. A
  failed connect drops the cached route, so the next request asks who owns the shard instead of
  waiting out a connect timeout on the dead broker every time (GetFelix/felixd#68).
