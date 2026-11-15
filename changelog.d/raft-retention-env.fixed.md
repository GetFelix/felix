- A broker with `FELIX_DURABLE_RETENTION_BYTES` or `FELIX_DURABLE_RETENTION_SECONDS` set no longer
  fails to open its Raft stream shards. Raft logs take no retention yet, so the broker-wide bounds
  are left off them (#2602).
