- Raft: with heartbeats sent together, the append that carries only a new commit
  waits once, in the heartbeat window, instead of the 1 ms commit hold and then
  the window (#3224); a longer hold is held only for its excess over the window. It is taken back from the window if the next append comes
  first, so followers learn a lone commit about 1 ms sooner, and a window opened
  for one heartbeat spawns no task.
