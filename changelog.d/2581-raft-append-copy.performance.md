- A Raft append no longer copies its record list on the way into the log, and sizes its
  record vectors once. A 64-record append allocates about 190 fewer bytes a record. A
  replicated (hybrid) follower storing a leader's batch skips the same copy (#2581).
