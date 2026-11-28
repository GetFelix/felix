- A Raft leader with a read waiting on a read-index round sends that group's heartbeats on
  their own instead of in a batch. Under bulk shard creation a follower's member could answer
  its heartbeat 200 to 800 ms late, and in a batch an answer that late is refused as slow, so
  the round found no quorum, waited out the 1.5 s lease and failed. The client then asked
  again through another broker, and lease and cache reads took 2 to 3.3 s (#3032).
