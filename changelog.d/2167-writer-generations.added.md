- Replication carries the generation that wrote each record (#2167). A new
  peer capability, `writer_generations`, adds the writers to a replication
  batch and a tail fetch; a follower on segment format v9 stamps each record
  with them instead of its own accepted generation, and refuses a batch that
  does not name them. A record below the log's generation floor is refused
  as a divergence rather than raised to it, and a leader whose accepted
  generation moved past its own is refused instead of stamping the newer one.
  `record_generations` is still not reported by brokers, so nothing changes
  on disk yet. Without the capability, peers exchange the same frames as
  before.
