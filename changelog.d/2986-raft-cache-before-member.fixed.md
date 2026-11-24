- On a broker that runs its shards on Raft, a cache, counter or consumer-group operation for
  a shard whose Raft member is not open yet (at startup, or before placement reaches the
  broker) is refused as not ready and retried, instead of being written to a hybrid store,
  where nothing would serve it and its directory kept the shard from ever opening on Raft. A
  forwarded one is refused the same way on the owner. No hybrid store, and not the broker's
  own stream storage, opens a durable shard's log by itself on such a broker.
