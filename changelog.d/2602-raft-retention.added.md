- Retention applies to Raft stream shards. The shard's leader reads the stream's control-plane `retention`, or the
  broker-wide `FELIX_DURABLE_RETENTION_BYTES` and `FELIX_DURABLE_RETENTION_SECONDS`, and proposes a head trim to the
  segment boundary they name, so every member hides the same records. It needs the fleet to have finalized
  `raft_head_trim`. The records stay on disk until Raft logs can purge (#2602). Raft logs no longer offload.
