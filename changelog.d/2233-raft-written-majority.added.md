- A `Quorum` write on a stream that does not fsync on commit can be acknowledged once a majority
  has written it (#2233). A follower tells the leader its written position before its fsync, over
  the peer transport, when the leader's append asks. `raft::RaftEngine::await_quorum_ack` returns
  at the commit or once a majority of the voters has written the record, whichever is first. The
  count takes voters only, both halves of a joint membership, and only reports at the leader's
  own term. The commit index, votes and the read bound still use fsynced positions.
  `docs/formal/FelixRaftWritten.tla` models it. Behind the `raft` feature; no shard is routed to
  it.
