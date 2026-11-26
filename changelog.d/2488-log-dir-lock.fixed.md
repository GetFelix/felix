- Every log directory is now locked while anything writes in it, not only
  while a Raft log is open. Stream, cache, counter and dead-letter logs, every
  `felix-log-tool` mode, `felix-broker restore-point`, and a Raft copy's open,
  stop, re-form start and group-of-one re-form take an exclusive `flock` on
  the directory before their first check and keep it until their last write.
  A second broker on the same data directory, or the log tool on a directory
  a broker holds, is refused with an error naming the directory instead of
  repairing a tail that is still being written. Two brokers are caught at
  each log as it opens, not at startup (#3250). The lock goes with the
  process, so a crash leaves nothing to clean up (#2488).
- Behaviour change: a broker whose data directory is on a filesystem that
  cannot lock a directory, such as NFS on Linux, no longer starts. It checks
  the lock at startup, before it is ready, and the error names the directory
  and says to keep it on a local disk.
