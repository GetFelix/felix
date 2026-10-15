- Replay rings and cache watch queues are bounded by bytes as well as entries
  (#1201). `FELIX_REPLAY_RING_MAX_BYTES` (64 MiB per stream shard) drops a
  ring's oldest records first; `FELIX_CACHE_WATCH_MAX_BYTES` (64 MiB per watch)
  ends a watch that would go over as lagged, as a full queue does. Each has an
  optional broker-wide `_TOTAL`. New gauges: `felix_replay_ring_bytes`,
  `felix_cache_watch_queue_bytes`.
