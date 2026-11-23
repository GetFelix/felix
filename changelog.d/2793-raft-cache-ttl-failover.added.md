- A felix-cluster test now checks that an entry put with a TTL on a Raft cache
  shard expires on the leader that takes over after a failover and is never
  read after its expiry (#2793).
