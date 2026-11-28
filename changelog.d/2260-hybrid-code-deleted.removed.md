- The hybrid shard engine's code is deleted from `felix-replication` and the broker
  service (#2260): its replication driver, quorum marks, replica reports, promotion and
  joint fences, elections and the simulator's hybrid clusters. Nothing ran it since
  #2259.
- Breaking for dashboards: these broker metrics are gone, since only the hybrid fed
  them: `felix_broker_replication_lag_records`, `_replication_halted`,
  `_replication_rebuilding`, `_replication_rebuilds_total`, `_replication_shipped_total`,
  `_replication_scans_total`, `_replication_passes_waiting`,
  `_replication_slot_wait_seconds`, `_replication_drain_withheld_total`,
  `_replication_move_throttled_bytes_total`, `_replicated_total`,
  `_replica_reports_per_request`, `_quorum_marks_withheld_total`,
  `_publish_quorum_failed_total`, `_publish_quorum_wait_seconds`,
  `_promotions_opened_total`, `_promotion_truncated_total`, `_fence_set_unknown_total`,
  `_fence_refused_ids_beyond_two_total` and `_shard_confirmed_reads_total`. The shipped
  Grafana dashboard shows the `felix_broker_raft_*` metrics in their place, and the
  alert rules `FelixReplicationHalted` and `FelixQuorumPublishTimeouts` are replaced by
  `FelixRaftShardStopped`.
- A cache watch that asks for both `retained` and `from_offset` is refused as invalid on
  every broker, before routing.
