- On the Raft shard engine, draining a broker now moves a shard it leads with replication
  factor 1 to a live broker, so the shutdown hand-off finishes (#3301). The set grows to
  both brokers and shrinks to the new one once its copy votes, so the draining broker stays
  leader and voter until then. A shard whose members all drain swaps a follower for a live
  broker first instead of waiting.
