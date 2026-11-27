- A Raft follower that answers a batch of heartbeats no longer refuses a group that answers a
  few milliseconds after the first one. It waits up to a quarter of the heartbeat after the
  first answer, where it used to wait 1 ms past the latest. On a broker stalled by bulk shard
  creation most batches lost a group this way, and a read-index round that needed that group's
  ack found no quorum and started again, so lease and cache reads took 0.5 to 1.8 s (#3032).
