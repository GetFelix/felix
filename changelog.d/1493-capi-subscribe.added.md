- The C ABI (`crates/sdk/felix-capi`) subscribes to a named shard and to every shard of a stream
  (#1493). `felix_client_subscribe_sharded` and `felix_client_resubscribe_sharded` return a
  `FelixShardedSubscription` read with `felix_sharded_next`, which delivers records and
  lost, recovered, moved and stream-deleted notices per shard; `felix_sharded_positions` gives the
  offsets to resume from. `felix_client_offset_for_time` finds the offset for a time,
  `felix_subscription_close_reason` tells an ended stream from a deleted one, and
  `felix_subscription_start_offset` returns where a subscription began.
