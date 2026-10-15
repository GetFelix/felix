- A broker that held a deleted stream's shard now takes the recreated stream's
  assignment even when its generation is lower, as after a control-plane
  restore rewinds the change seq (#1209). Shard ownership is ordered by
  `(incarnation, generation)`, as the shard lifecycle already was. Deleting a
  namespace or tenant on the in-memory or Raft control plane now also removes
  its streams' and caches' shard assignments and replica reports, as deleting
  the stream itself and Postgres's cascade already did; before, a stream
  created again under the name was assigned as an update of the deleted one's
  shard.
