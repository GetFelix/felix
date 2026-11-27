- Breaking: Raft is the only shard engine at run time (#2259). A broker or control plane
  started with `FELIX_SHARD_ENGINE=hybrid` refuses to start and says so; `raft` or unset
  start as before. No broker starts the hybrid's replication driver, suspicion heartbeat,
  election driver or promotion fence. A broker built without the `raft` feature refuses
  to start unless it is standalone with no durable storage.
- Retired with the hybrid, reported at start by name and otherwise ignored:
  `FELIX_SHARD_ENGINE`, `FELIX_INTERNAL_ELECTION`, `FELIX_INTERNAL_FENCE`,
  `FELIX_UNIFIED_FENCE`, `FELIX_ELECTION_DEFER_FOR_MS`, `FELIX_LEADER_SUSPECT_AFTER_MS`,
  `FELIX_SHARD_SUSPECT_AFTER_MS`, `FELIX_SHARD_FENCE_TIMEOUT_MS`,
  `FELIX_SHARD_PENDING_SET_TIMEOUT_MS`, `FELIX_SHARD_ELECTION_HOLD_MS`,
  `FELIX_SHARD_STUCK_LEADER_MS` and `FELIX_SHARD_MOVE_FENCE_MAX_LAG_RECORDS`.
- The history campaign has one mode: `FELIX_HISTORY_MODE` and the `lease` and
  `lease-free` modes are gone. The hybrid's cluster jobs, nightly simulator modes and
  `perf-raft-rf3.yml` are dropped.
