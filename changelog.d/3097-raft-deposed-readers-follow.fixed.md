- A Raft leader that is deposed, or a member that knows no leader, now ends its subscribers
  with `shard_moved` (no `resume_from`, no owner named) instead of a clean end, so a
  cluster client's subscription follows the shard to wherever it is served next, resuming
  after the last record it saw.
