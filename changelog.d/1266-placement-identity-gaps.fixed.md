- A Raft control plane restarted with one member down no longer drops storage
  identities from registrations and reports until that member answers. A
  member that has not answered counts at the level the log already depends on,
  not 0. Before, a wiped follower's registration then cleared its identity and
  an older report could promote it empty.
- A follower without a storage identity now counts only from a report taken
  at its current incarnation. The control plane stamps each report with the
  leader that made it and every named node's registered incarnation.
- A leader that dies partway through a move no longer leaves its last report
  to expire because the move's steps raised the generation (#1250). The
  report stays believed while its leader is still the assignment's leader.
- A `Leader` shard is no longer promoted on a previous leader's report once
  another node leads: what the new leader acknowledged may be on it alone. A
  new leader that dies before its first report leaves the shard waiting for
  it; before, it was promoted on the old report for a while and then stalled.
  `Quorum` shards still promote on it. Postgres migration
  `0031_replica_report_stamp`; under Raft, metadata level 13.
