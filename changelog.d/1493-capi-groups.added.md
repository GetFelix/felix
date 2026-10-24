- The C ABI (`crates/sdk/felix-capi`) runs consumer groups on a shard (#1493): `felix_client_group_poll`
  with a named consumer, visibility timeout and reclaim, the settles `_ack`, `_nack` (with a delay),
  `_extend`, `_dead_letter`, `_discard` and `_redrive`, and `_create`, `_seek`, `_describe`, `_delete`
  and `_dead_letters`. `felix_client_stream_shards` returns a stream's shard count so a caller can run
  one group per shard.
