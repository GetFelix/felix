- The C ABI (`crates/sdk/felix-capi`) covers the rest of the Rust client surface (#1493): `felix_client_group_sharded`
  with `felix_sharded_group_poll` for a consumer group across every shard, the stream-wide
  `felix_client_group_create_stream`, `_seek_stream`, `_describe_stream` and `_delete_stream`, and
  `felix_client_connect_with` with `FelixConnectOptions` so a client can ask for each record's publisher and append time,
  read with `felix_event_publisher`, `felix_event_timestamp` and the new `FelixGroupRecord` fields. `FelixGroupRecord`
  grew four fields, which changes its layout; regenerate bindings from `felix.h`.
