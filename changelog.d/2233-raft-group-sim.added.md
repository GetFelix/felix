- felix-sim runs groups of the per-shard Raft engine on virtual time (#2233): `felix_sim::raft`,
  with link faults, skewed timers, live subscribers, the history checker and group checks, and a
  bound that fails a stalled run. Test-only, behind the crate's `raft` feature. See "Raft groups"
  in `docs/simulation.md`.
