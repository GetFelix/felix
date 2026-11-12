- A runtime for per-shard Raft groups (#2231), behind the `raft` cargo feature of
  `felix-replication`, which no release enables: many openraft groups on one broker, each on a
  shard's log, with one ticker for all of them in place of a timer per group. No shard runs on
  it yet.
