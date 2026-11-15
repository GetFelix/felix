- A broker's Raft members batch their log marks: one synced write a second to the data root's
  `raft-marks` file covers every shard whose flushed position moved, instead of a synced write
  and a run-witness rewrite on every flush round of every member (#2576). At RF 3 with six
  shards, Quorum publish throughput rose 23% (fsync on commit) and 27% (periodic fsync), and
  batch ack latency fell by about a fifth. Vote marks are still
  synced before a vote is answered. A single shard directory, or a whole root in the same run,
  put back to a state no older than the last mark write (about a second) with no vote since is
  no longer detected; older restores still are (`docs/per-shard-raft.md`, "What this does not catch").
  A batch still unwritten five seconds after the last one started stops the root's appends and votes,
  as a failed mark write does, and `felix_storage_raft_log_mark_age_seconds` shows how far behind
  the marks are. A planned stop writes one last batch. The exact mark is kept for tests.
