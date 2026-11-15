- A broker with `FELIX_DURABLE_RETENTION_BYTES` or `FELIX_DURABLE_RETENTION_SECONDS` set no longer
  fails to open its Raft stream shards. A Raft log takes no retention bounds of its own, so the
  broker-wide bounds are left off it; its leader enforces them with head trims instead (#2602).
