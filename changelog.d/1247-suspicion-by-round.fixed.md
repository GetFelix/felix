- Placement takes a follower's word against a leader only when that leader's
  own replica report says it serves the shard by round (`by_round`): it
  acknowledges by its followers and confirms its reads by a round (#1247).
  Before, a leader still reading on its lease could be replaced while that
  lease was valid and keep serving consumer-group writes and state reads,
  either because it ran with `FELIX_QUORUM_READS=lease` or because it had not
  yet heard on a heartbeat that the fleet finalized `lease_free_reads`. A
  report without the field, from an older broker, reads as the lease. Postgres
  migration `0029_replica_report_by_round`; under Raft, metadata level 11.
- A broker started with `FELIX_QUORUM_READS=lease` no longer reports
  `lease_free_reads`. The finalize dry run names it, and the finalize is
  refused while it serves; once the feature is finalized such a broker is
  refused at registration, like one with `FELIX_INTERNAL_FENCE=false`.
