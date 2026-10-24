- A write can name its shard instead of hashing a key (#1465). `publish`, `publish_batch`,
  `publish_idempotent`, `publish_if`, `commit` and `state_get` take an optional `shard`,
  negotiated as `FEATURE_EXPLICIT_SHARD` (bit `0x8` of the extended feature word). A shard the
  stream does not have is refused with `invalid_request` and nothing is written. The Rust client
  adds `publish_on_shard` and `publish_batch_on_shard` (`Publisher`, `ClusterClient`), and
  `commit_on_shard`, `commit_if_on_shard`, `publish_if_on_shard` and `state_get_on_shard`
  (`Client`, `ClusterClient`). `ClusterClient` sends each to the named shard's owner or leader.
  They fail without sending against a broker that did not advertise the bit. Caches and counters
  take no shard.
