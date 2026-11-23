- A Raft vote at a new term is saved with half the fsyncs: the ballot file
  alone carries the raised term, and the replica state follows with the next
  commit write. When 2,000 groups lose their leader together on a disk held to
  5 ms an fsync, the model of the election bound has them all led in 23 s
  instead of 43 s (#2317).
