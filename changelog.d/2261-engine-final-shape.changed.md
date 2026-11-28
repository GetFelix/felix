- The replication engine trait, the write fence and the shard lifecycle lose what only the
  hybrid used (#2261). `Engine` no longer has `appended`, `committed_bound`, the cache and
  counter waits, `read_skips_lease` or `await_readable`; `committed_bound` and
  `CommittedReads` moved to the broker service's `StreamEngine`. The lifecycle has no
  `Fencing` phase, so the `felix_broker_shard_phase` gauge no longer reports a `fencing` series,
  and `shard_inspect` never answers `fencing`. An interim routing publish no longer copies
  the assignment map under the lifecycle lock (#2133). No behaviour changes otherwise.
- Code the hybrid's removal left without a caller is gone too: `serving::direct_cache`,
  the `StreamLog` ballot and replica-set wrappers (`accept_set*`, `accept_generation_on_set`,
  `give_up_generation`, `admit_member` and the rest), `Broker::copy_on_disk`,
  `note_replicated_commit`, `shard_log_at` and the `mark_*_for` helpers,
  `ConsumerGroups::adopt`, `ShardRouter::replica_role` with `ReplicaRole`, and the confirmed
  assignment reads on `ControlPlaneLink`.
