- Metrics for what a `Quorum` acknowledgement waits on (#1479):
  `felix_broker_publish_quorum_wait_seconds`, how long a publish waited for its
  majority, and `felix_storage_replica_state_writes_total` with
  `felix_storage_replica_state_write_duration_seconds`, the replica-state writes
  a follower makes for commit offsets.
