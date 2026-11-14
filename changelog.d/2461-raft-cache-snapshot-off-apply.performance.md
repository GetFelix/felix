- A Raft cache group builds its snapshot without holding apply (#2461): the state machine freezes
  its maps between entries without copying them, and the frozen maps are encoded and written on a
  blocking thread while entries keep applying to an overlay that is folded back afterwards. A
  member that receives a snapshot parses it once, on a blocking thread, and installs the state it
  parsed (#2467).
