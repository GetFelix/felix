- A runtime for per-shard Raft groups (#2231), behind the `raft` cargo feature of
  `felix-replication`, which no release enables: many openraft groups on one broker, each on a
  shard's log, with one ticker for all of them in place of a timer per group. No shard runs on
  it yet.
- The runtime slows an idle group's heartbeat to once a second after 5 s without an entry, and
  reports each log's written and flushed positions for the publish path to wait on (#2231).
