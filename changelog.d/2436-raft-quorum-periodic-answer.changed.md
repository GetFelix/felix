- On a Raft shard, a `Quorum` stream under `Periodic` or `None` fsync now
  answers its publisher once a majority has written the batch, before their
  fsync, as the hybrid does (#2436). Under `OnCommit` it is still answered at
  the commit, and subscribers get the batch at its commit either way. Such a
  batch is lost only if a majority of copies each lose their unsynced tail,
  by a power cut or a failed flush. Stream reads stay bounded by the commit;
  `RaftEngine::confirm_read` waits until the leader has applied the answered
  batch and refuses a read whose shard closed during that wait, but no stream
  read calls it yet.
