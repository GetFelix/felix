- Breaking: Raft is the default shard engine (#2167). With `FELIX_SHARD_ENGINE` unset, a
  control plane forms new clusters for Raft, and a broker with a node id, the
  broker-internal listener and durable storage runs its durable stream shards on Raft.
  The release build includes the engine (the `raft` cargo feature of
  `felix-broker-service` is on by default). A standalone or in-memory broker, which
  cannot run Raft, keeps its shards on the hybrid path.
