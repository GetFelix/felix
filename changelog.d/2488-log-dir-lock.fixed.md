- Every log directory is now locked while anything writes in it, not only
  while a Raft log is open. Stream, cache, counter and dead-letter logs, every
  `felix-log-tool` mode, `felix-broker restore-point`, a Raft copy's open,
  stop, re-form start and group-of-one re-form, the switch of a stream from
  the hybrid engine to a Raft voter, the claim that marks a directory as a
  Raft member's, and the write of a fresh epoch take an exclusive `flock` on
  the directory before their first check and keep it until their last write.
  A second broker on the same data directory, or the log tool on a directory
  a broker holds, is refused with an error naming the directory instead of
  repairing a tail that is still being written. Two brokers are caught at
  each log as it opens, not at startup, and the data directory's own files
  (the run record, the storage id and the marks) are not locked, so a second
  broker may write its run record and rotate the storage id under the first
  before a log refuses it (#3250). A close or a shutdown of a log lets go
  of the lock before it returns, so a broker stopped in the same process
  can be started again at once. The lock goes with the process, so a crash
  leaves nothing to clean up (#2488).
- Behaviour change: a broker whose data directory is on a filesystem that
  cannot lock a directory, such as NFS on Linux, no longer starts. It checks
  the lock at startup, before it is ready, and the error names the directory
  and says to keep it on a local disk.
