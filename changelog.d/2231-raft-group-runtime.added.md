- A runtime for per-shard Raft groups (#2231), behind the `raft` cargo feature of
  `felix-replication`, which no release enables: many openraft groups on one broker, each on a
  shard's log, with one ticker for all of them in place of a timer per group. No shard runs on
  it yet.
- The runtime slows an idle group's heartbeat to once every 2 s after 5 s without an entry, and
  reports each log's written and flushed positions for the publish path to wait on (#2231).
- The runtime keeps every record under a snapshot and no member installs one from a peer, a
  close holds its shard until the member has stopped, a leader heartbeats fast again when a
  follower misses a heartbeat, and a ticker that panics is restarted and counted in
  `felix_broker_raft_ticker_restarts_total` (#2231).
