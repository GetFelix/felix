- A publish on a Raft shard that is answered before its commit now finishes on the task that answered it,
  not on a new one (#2577). `FELIX_HOP_TRACE=1` prints how long a publish takes to reach each stage.
- A Raft follower handles each append and vote on the task that received it instead of spawning one per request
  (#2577). One the sender stops waiting for still finishes on a task of its own, so it still restarts the election timer.
