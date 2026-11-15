- A broker's Raft members batch their log marks: one synced write a second to the data root's
  `raft-marks` file covers every shard whose flushed position moved, instead of a synced write
  and a run-witness rewrite on every flush round of every member (#2576). At RF 3 with six
  shards, Quorum publish throughput rose 23% (fsync on commit) and 27% (periodic fsync), and
  batch ack latency fell by about a fifth. Vote marks are still
  synced before a vote is answered. A single shard directory, or a whole root in the same run,
  put back to a state no older than the last mark write (about a second) with no vote since is
  no longer detected; older restores still are (`docs/per-shard-raft.md`, "What this does not catch").
  A failed mark write stops the root's appends and votes. A mark write that hangs fails nothing:
  once the marks are five seconds old the root's appends wait for them (votes wait on the same
  writer) and resume when the disk does, so no ack is more than about six seconds ahead of its
  mark. `felix_storage_raft_log_mark_age_seconds` shows how far behind the marks are. A planned
  stop writes one last batch per data root. The exact mark is kept for tests.
