- `FELIX_SHARD_ENGINE` (`hybrid` by default) names the engine that replicates
  a broker's durable stream shards (#2233). `raft` is refused at startup for
  now, with the reason: no build routes a shard to Raft yet, and it will need
  the `raft` cargo feature, a cluster identity, the broker-internal listener
  and durable storage. An unknown value is refused instead of read as the
  default.
