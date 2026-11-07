- C and C# clients reach parity with the Rust client for inspection, subscription counters,
  queue and transport options, and producer hand-offs. C: `felix_client_supports`,
  `felix_client_inspect_shard`, `felix_client_list_subscriptions`
  (`FelixSubscriptionFilter`), `felix_client_topology`, `felix_client_stream_routing`,
  `felix_client_shard_owners` and `felix_client_cache_shards`, which answer structured data as
  one JSON string with the wire's field names;
  `felix_subscription_moves`, `felix_subscription_reconnects`,
  `felix_subscription_stream_deleted` and `felix_subscription_queue_capacity`;
  `felix_producer_hand_off_batch`, `felix_producer_hand_off_batches`,
  `felix_producer_hand_off_abandon` and the `FelixHanded` handle
  (`felix_handed_offset`, `felix_handed_done`, `felix_handed_free`); and new
  `FelixConnectOptions` fields at the end of the struct for the subscription queue policy and
  capacities, publish and cache pool sizes, QUIC windows, `max_frame_bytes`, `ack_on_commit`
  and the connect and finish timeouts. C#: `GetSupportsAsync`, `InspectShardAsync`,
  `ListSubscriptionsAsync`, `GetTopologyAsync`, `GetStreamRoutingAsync`,
  `GetShardOwnersAsync` and `GetCacheShardsAsync` with typed records,
  `FelixSubscription.Moves` / `Reconnects` / `StreamDeleted` / `QueueCapacity`,
  `FelixProducer.HandOffBatchAsync` / `HandOffBatchesAsync` / `HandOffAbandonAsync` returning a
  `Handed`, and the same options as properties of `FelixClientOptions`. The client fixture
  file gains `inspector_token`.
