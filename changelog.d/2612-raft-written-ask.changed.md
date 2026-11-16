- Under `raft_written_commit`, a Raft leader on a relaxed broker no longer
  asks followers on relaxed brokers for a written report: their append
  answer already comes at the write and counts toward the commit, so the
  follower skips the marker sync and the `Request::Written` message.
  Followers that fsync before answering still report, and a leader that
  fsyncs first still asks every follower (#2612).
