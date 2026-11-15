- A broker's Raft members batch their log marks: one synced write a second to the data root's
  `raft-marks` file covers every shard whose flushed position moved, instead of a synced write
  and a run-witness rewrite on every flush round of every member (#2576). Vote marks are still
  synced before a vote is answered. A single shard directory, or a whole root in the same run,
  put back to a state from the last second with no vote since is no longer detected; older
  restores still are (`docs/per-shard-raft.md`, "What this does not catch").
