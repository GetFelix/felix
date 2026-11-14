- A shard's Raft leader now sends up to 1,024 entries in one AppendEntries, up from 300 (#2532).
  One message is about 1 MiB at 1 KiB records. A follower with a backlog catches up in fewer round trips,
  and delivery p99 dropped about threefold in the measurement on the issue.
