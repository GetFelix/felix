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
- Under Raft, the leader refuses to add a control-plane member whose build is
  below the metadata version the group already uses, or that reports none;
  the error names both versions. That version is the one a finalized fleet
  feature needed (10 for `stream_incarnations`), or the newest one any
  committed entry needed, which the state machine now tracks and carries in
  its snapshot (#1209). A member already in the group that comes back on an
  older build, as after a lost volume, is no longer sent entries or a
  snapshot once it reports an older version, or while it has lost its log
  and not yet reported one; the leader logs it once and sets
  `felix_meta_raft_member_below_floor{member}` to 1. It still gets
  heartbeats, so it does not call an election. A member that is slow or
  unable to answer is not held. Either member would refuse entries the
  others apply and diverge from them. A held voter is one fewer failure the
  group survives until it is upgraded. A group that ran unreleased builds of
  main may have members whose tracked level differs, since those builds
  applied some entries without raising it; while such a member leads, it can
  admit or feed a member the others would hold.
