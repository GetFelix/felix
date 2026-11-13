- Raft shards deliver committed records only (#2233). `raft::RaftEngine::publish` proposes a
  batch through the stream's new hand-over order (`Broker::propose_publish`), and the batch is
  fanned out from the publisher's own bytes, in offset order, once it is committed under the
  term that wrote it. `raft::RaftReads` bounds every reader at the commit index, at every
  consistency level, and `RaftEngine::follow_commits` wakes them as it moves. Behind the `raft`
  feature; no shard is routed to it.
