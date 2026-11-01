- The control plane's Raft store now commits on tokio's blocking pool instead
  of an async worker (#1705). Each commit fsyncs, and under disk load a slow
  flush stalled the heartbeats queued on the same worker long enough to start
  an election. Write order and durability are unchanged.
