- While one broker is down, `Quorum` writes to the brokers still up no longer
  time out under a steady load (#1944). Every append made the replication
  driver ask for a pass on each shard with a follower behind, which with a
  follower down is every shard it follows, and those passes took the slots
  from the shards with writes waiting. An append now asks only for its own
  shard's pass and for shards owed a mark that a follower can still answer
  for; the tick retries the rest.
