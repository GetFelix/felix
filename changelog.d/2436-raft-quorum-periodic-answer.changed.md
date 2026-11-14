- On a Raft shard, a `Quorum` stream under `Periodic` or `None` fsync now
  answers its publisher once a majority has written the batch, before their
  fsync, as the hybrid does (#2436). Under `OnCommit` it is still answered at
  the commit, and subscribers get the batch at its commit either way. A read
  confirmed after such an answer waits until the leader has applied the
  answered batch.
