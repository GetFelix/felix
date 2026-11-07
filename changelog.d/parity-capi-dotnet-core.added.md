- C and C# clients reach parity with the Rust client for record headers,
  lease-token writes, per-principal identities, page reads, topology refresh and the
  subscription live offset. C: `felix_client_publish_with_headers`,
  `felix_client_publish_batch_with_headers` (`FelixRecord`, `FelixHeader`),
  `felix_event_key` / `felix_event_header_count` / `felix_event_header`,
  `felix_client_cache_put_if_lease`, `felix_client_with_identity`, `felix_client_read`,
  `felix_client_refresh_topology`, `felix_client_connections_per_node` and
  `felix_subscription_live_offset`. `FelixConnectOptions` gains
  `disable_record_headers`, so record headers are offered unless it is set. C#:
  `PublishWithHeadersAsync`, `PublishBatchWithHeadersAsync`, `FelixEvent.Key` and
  `Headers`, `PutIfAbsentLeaseAsync` / `PutIfVersionLeaseAsync`, `WithIdentityAsync`,
  `ReadRangeAsync`, `RefreshTopologyAsync`, `GetConnectionsPerNodeAsync` and
  `FelixSubscription.LiveOffset`. A refused record header maps to
  `FELIX_STATUS_INVALID_ARGUMENT` and an unsupported broker to
  `FELIX_STATUS_UNSUPPORTED`. The client fixture now finalizes `record_headers`.
