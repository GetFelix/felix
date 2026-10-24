- A subscribe from `latest` on a durable stream whose log ends in generation-start
  records now reports `start_offset` and `live_offset` just past the last event,
  not at the raw tail (#1475). Those records are never delivered, so a client that
  read history up to `live_offset - 1` after joining at `latest` waited forever,
  which on a 0.6.0 cluster is any Quorum shard with no write since its leader
  started. A client that negotiated `FLAG_EVENT_BATCH_SKIPPED` counts no drop at
  the join. A client with offsets but without it now reads one false drop per
  trailing generation start at a `latest` join, as it already did mid-stream.
  Count drops before the first event as `offset - start_offset - skipped_before`:
  `start_offset` can be 0.
