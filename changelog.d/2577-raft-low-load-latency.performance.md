- A publish on a Raft shard that is answered before its commit now finishes on the task that answered it,
  not on a new one (#2577). `FELIX_HOP_TRACE=1` prints how long a publish takes to reach each stage.
