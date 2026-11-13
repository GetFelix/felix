- `FELIX_SHARD_ENGINE` (`hybrid` by default) names the engine that replicates
  a broker's durable stream shards (#2233). With `raft`, each durable stream
  shard is one Raft group: publishes commit through the group, subscribers are
  served on its leader (a follower answers with a redirect to it), and a
  stopped leader is replaced by election. It is for fresh clusters only, every
  broker must say the same, and it needs a build with the `raft` cargo
  feature, a cluster identity, the broker-internal listener and durable
  storage; a broker missing one refuses to start and says which. A stream on
  Raft takes plain publishes only: idempotent and conditional publishes,
  atomic commits, head trims, record headers, consumer groups, Kafka produce
  and MQTT publish are refused by name (#2446). An unknown value is refused
  instead of read as the default.
