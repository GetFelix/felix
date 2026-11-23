- A `Quorum` publish on a Raft shard whose followers do not fsync on commit
  no longer costs each follower a task and a written-report message per
  append (#2936). The follower's answer to the append already comes at the
  write, so the leader counts that answer as the report.
