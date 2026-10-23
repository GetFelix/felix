- A fenced shard move whose leader is live but never reports drained no longer holds the shard
  without writes forever (#1434). After `FELIX_SHARD_FENCE_TIMEOUT_MS` (two minutes by default,
  `shard_fence_timeout_ms` in the config file) placement gives the move up the way an operator's
  cancel does: the leader serves again at a new generation, holding every write it accepted, and
  the move counts in `felix_shard_moves_timed_out_total`. `0` keeps the old behaviour.
