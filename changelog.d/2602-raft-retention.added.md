- Retention applies to Raft stream shards. The shard's leader reads the stream's control-plane `retention`, or the
  broker-wide `FELIX_DURABLE_RETENTION_BYTES` and `FELIX_DURABLE_RETENTION_SECONDS`, and proposes a head trim to the
  segment boundary they name, so every member hides the same records. The trim is not conditional on the tail, so a busy
  shard's publishes cannot keep refusing it. It needs the fleet to have finalized `raft_head_trim`; a pass that cannot
  trim counts `felix_broker_raft_retention_trims_refused_total`. The records stay on disk until Raft logs can purge
  (#2602). Raft logs no longer offload.
