- Breaking: Raft is the default shard engine (#2167). With `FELIX_SHARD_ENGINE` unset, a
  control plane forms new clusters for Raft, and a broker with a node id, the
  broker-internal listener and durable storage runs its durable stream shards on Raft.
  The release build includes the engine (the `raft` cargo feature of
  `felix-broker-service` is on by default). A broker with a node id that cannot run
  Raft (no internal listener, no durable storage, or a build without the feature)
  refuses to start and names what is missing, and every registering broker is
  compared with the control plane's engine, with or without durable storage. A
  standalone broker (no node id) keeps its durable streams on the hybrid path and
  logs a warning until the hybrid is removed.
