- A broker that held a deleted stream's shard now takes the recreated stream's
  assignment even when its generation is lower, as after a control-plane
  restore rewinds the change seq (#1209). Shard ownership is ordered by
  `(incarnation, generation)`, as the shard lifecycle already was. Deleting a
  namespace or tenant on the in-memory or Raft control plane now also removes
  its streams' and caches' shard assignments and replica reports, as deleting
  the stream itself and Postgres's cascade already did; before, a stream
  created again under the name was assigned as an update of the deleted one's
  shard. On Raft, deletes take shard assignments and replica reports this way
  only once every member is at metadata version 10, the same gate as stream
  incarnations; below it every member keeps what the older release keeps, so
  members never disagree.
- Under Raft, once a fleet feature is finalized the leader refuses to add a
  control-plane member whose build is below the metadata version that
  finalize needed (10 for `stream_incarnations`), or that reports none; the
  error names both versions. Such a member would refuse entries the others
  apply and diverge from them. Before any finalize an older build may still
  join (#1209).
