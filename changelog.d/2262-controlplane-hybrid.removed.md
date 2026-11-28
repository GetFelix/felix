- Breaking: the control plane no longer serves `GET` or `POST
  /v1/shard-assignments/confirmed` (#2262). Only the hybrid's brokers asked it, before
  they voted for a leader. A Raft group elects its own leader from its own log.
- The control plane keeps no leader suspicions. A heartbeat's `suspects` list is no
  longer read, and placement no longer fails a shard over on its followers' word. The
  `shard_election_hold_ms` config key is retired: still accepted, reported at start, and
  without effect.
- Breaking: `POST /v1/nodes/{node_id}/replica-status` is gone, with the
  `felix_replica_status_rejected_total` metric. Only the hybrid's leaders reported
  there; a Raft group reports through `raft-floors` and `raft-copy-id`.
