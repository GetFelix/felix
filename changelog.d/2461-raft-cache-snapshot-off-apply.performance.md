- A Raft cache group builds its snapshot without holding apply (#2461): the state machine takes a
  copy of its state that shares memory with the live one, and the copy is encoded and written on a
  blocking thread while entries keep applying. A member that receives a snapshot parses it once,
  on a blocking thread, and installs the state it parsed (#2467).
