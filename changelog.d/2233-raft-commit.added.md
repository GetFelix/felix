- The Raft engine says what is committed (#2233). A shard's state machine publishes the commit
  index as it applies entries. `raft::RaftEngine::await_commit` waits on it and checks the term
  of the entry at the write's offset, `read_bound` is the commit index plus one, and
  `confirm_read` is a read-index round. A leader reports `opened` once the blank entry of its
  term is committed and `deposed` when it stops leading that term. Behind the `raft` feature; no
  shard is routed to it.
