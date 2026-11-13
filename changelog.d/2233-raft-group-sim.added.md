- felix-sim runs groups of the per-shard Raft engine on virtual time (#2233): `felix_sim::raft`,
  with link faults, skewed timers, the history checker and group checks. Test-only, behind the
  crate's `raft` feature. See "Raft groups" in `docs/simulation.md`.
