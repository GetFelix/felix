- The Raft engine takes publishes (#2233). `raft::RaftEngine::propose` queues a batch in the
  shard's commit order, and one task per shard hands everything queued to openraft in one
  `client_write_many`. The publisher is answered once its records are written, or fsynced on a
  stream that fsyncs on commit. A publish puts an idle group back on fast heartbeats. Behind the
  `raft` feature; no shard is routed to it.
