- The C ABI (`crates/sdk/felix-capi`) runs caches, counters and cache watches (#1493): `felix_client_cache_put`,
  `_get`, `_get_versioned`, `_delete`, `_put_if` and `_delete_if`, `felix_client_counter_add` and `_get`, and
  watches on a key or prefix of one shard (`felix_client_watch_cache`) or across every shard
  (`felix_client_watch_cache_sharded`), both optionally retained, read with `felix_cache_watch_next` and
  resumed from `felix_cache_watch_resume_offsets`.
