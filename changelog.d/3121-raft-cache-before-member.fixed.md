- On a broker that runs its shards on Raft, a cache or counter operation for a shard whose
  Raft member is not open yet (at startup, or before placement reaches the broker) is refused
  as not ready and retried, instead of being written to the hybrid's cache or counter store,
  where nothing would serve it and its directory kept the shard from ever opening on Raft.
  The hybrid's cache and counter stores open no log by themselves on such a broker.
