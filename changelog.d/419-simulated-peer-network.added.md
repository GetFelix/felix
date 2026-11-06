- A simulated peer network for deterministic cluster tests, in the new
  `felix-sim` test crate (#419). It carries peer requests in memory with
  seeded drops, delays and partitions per link, and refuses traffic from a
  crashed node's old incarnation. Replication's timers and the lease clocks
  can now run on virtual time. See `docs/simulation.md`.
