- Every log directory is now locked while it is open, not only a Raft log's. A
  stream, cache or counter log, a broker's stream root and every
  `felix-log-tool` mode take an exclusive `flock`, so a second broker on the same
  data directory, or the log tool on a directory a broker holds, is refused with
  an error naming the directory instead of repairing a tail that is still being
  written. A Raft copy's directory stays locked from the copy judge through a
  rotation into the open log (#2488).
