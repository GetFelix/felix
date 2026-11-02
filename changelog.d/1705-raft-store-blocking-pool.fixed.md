- The control plane's Raft store now commits on tokio's blocking pool instead
  of an async worker (#1705). Each commit fsyncs, and under disk load a slow
  flush stalled the other tasks on that worker. On a one-worker runtime, such
  as a control plane limited to one CPU, that was enough to start an
  election. Write order and durability are unchanged.
