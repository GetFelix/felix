- Every log directory is now locked while it is open, not only a Raft log's. A
  stream, cache or counter log, a broker's stream root and every
  `felix-log-tool` mode take an exclusive `flock`, so a second broker on the same
  data directory, or the log tool on a directory a broker holds, is refused with
  an error naming the directory instead of repairing a tail that is still being
  written. A Raft copy's directory stays locked from the copy judge through a
  rotation into the open log (#2488).
- Behaviour change: a stream, cache or counter log, and a broker's stream root,
  now refuse to open on a filesystem where the directory `flock` fails, such as
  NFS on Linux (where `flock` is emulated with byte-range locks a directory
  cannot take) or a mount without lock support. Before, only Raft logs refused.
  Keep the data directory on a local disk.
