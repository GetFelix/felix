- `FELIX_PUB_SHARD_STREAMS` (`publish_shard_streams`) now defaults to 4 instead of 16 (#1406). With
  keys spread over 48 shards, 256 B acked publishes measured about 125 MB/s at 4 and about 97 MB/s
  at 16. Behaviour change: only the first four shards a client publishes to on a broker (first come,
  across all streams) get a dedicated stream; later shards use the shared pool, where a stalled
  shard can hold up the other pooled shards. Raise the variable to the total number of shards the
  client publishes to on a broker for full isolation.
