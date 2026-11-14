- `felix-broker` can open a durable stream shard over a log that belongs to
  its shard's engine, a Raft group member (#2233):
  `Broker::open_stream_on_engine_log`. The stream starts at what the engine
  committed, refuses every write that takes its own offsets with
  `BrokerError::NotOnEngineStream` (#2446), and keeps its commit hold when
  the leader is deposed. `Broker::leave_durable_streams_to_engines` stops a
  broker opening a log of its own for a durable stream; a shard its engine
  has not opened answers `BrokerError::ShardNotOpen`. `propose_publish`
  refuses with `BrokerError::ProposalsStalled` while the oldest batch still
  waiting for its commit has waited past the publish timeout, and a reader
  resuming after a batch that was given up on starts above it (#2428).
  Nothing in the broker service opens a stream this way yet.
