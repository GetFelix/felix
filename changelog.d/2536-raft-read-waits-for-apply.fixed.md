- A read confirmed on a Raft shard now waits until the leader has applied the
  read index, and the reader takes its value after that (#2536). A `Leader`
  write answered on the lease could commit before the leader applied it, and
  a read in that window missed a write its client had already been answered.
