- The replication engine trait, the write fence and the shard lifecycle lose what only the
  hybrid used (#2261). `Engine` no longer has `appended`, `committed_bound`, the cache and
  counter waits, `read_skips_lease` or `await_readable`; `committed_bound` and
  `CommittedReads` moved to the broker service's `StreamEngine`. The lifecycle has no
  `Fencing` phase, so the `felix_broker_shard_phase` gauge no longer reports a `fencing` series,
  and `shard_inspect` never answers `fencing`. An interim routing publish no longer copies
  the assignment map under the lifecycle lock (#2133). No behaviour changes otherwise.
