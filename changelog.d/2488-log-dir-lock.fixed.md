- Every log directory is now locked while it is open, not only a Raft log's. A
  stream, cache, counter or dead-letter log and every `felix-log-tool` mode take
  an exclusive `flock`, so a second broker on the same data directory, or the
  log tool on a directory a broker holds, is refused with an error naming the
  directory instead of repairing a tail that is still being written. A Raft
  copy's directory stays locked from the copy judge through a rotation into the
  open log. The lock goes with the process, so a crash leaves nothing to clean
  up (#2488).
- Behaviour change: stream, cache and counter logs now refuse to open on a
  filesystem where a directory `flock` fails, such as NFS on Linux, as Raft
  logs already did. Keep the data directory on a local disk.
