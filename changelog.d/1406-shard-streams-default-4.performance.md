- `FELIX_PUB_SHARD_STREAMS` (`publish_shard_streams`) now defaults to 4 instead of 16 (#1406). With
  keys spread over 48 shards, 256 B acked publishes measured about 125 MB/s at 4 and about 97 MB/s
  at 16. Behaviour change: a stream with more than four shards now shares dedicated streams, so a
  stalled shard holds up the shards on its stream, not the whole stream's. Set the variable to the
  stream's shard count for full per-shard isolation.
