---
title: "Metrics Reference"
description: "Every Prometheus metric Felix registers, by component: name, type, labels and what it tells you."
---

Every Prometheus metric the broker, the control plane, the storage engine, the replication layer and the Rust client register. [Observability](/features/observability/) groups the common ones by the question they answer and shows alert and query examples; this page is the complete list.

Both services serve Prometheus text on their metrics endpoint. Client metrics are recorded in the process that embeds the client, so they appear on whatever exporter that application installs. A script checks this page against the metric names in the source (`scripts/check_metrics_reference.py`, run by `task docs:evidence`), so a metric that is added, renamed or removed without a matching edit here fails CI.

## Conventions

- Names ending in `_total` are counters, `_seconds` and `_ms` series are in those units, and `_ns` series are in nanoseconds.
- **Telemetry build only** means the series is recorded only when the broker or client is built with the `telemetry` feature. The default build records nothing for it.
- A label listed in the Labels column is the whole label set. Series with no labels have `-`.
- `tenant` labels are capped. See the tenant section.


## Broker: publish path

Counted at the QUIC publish handlers and the broker's publish workers. A publish is counted once per request, and a batch is one request.

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_publish_requests_total` | counter | `result` | Publish requests by result. A batch is one request. The base rate and error ratio for the publish path. |
| `felix_publish_bytes_total` | counter | - | Payload bytes of `ok` and `accepted` publish requests. |
| `felix_publish_latency_ms` | histogram | `mode` | Time from receiving a publish to answering it, in milliseconds. Telemetry build only. |
| `felix_broker_publish_claim_jobs` | histogram | - | Publishes folded into one durable claim. Near 1 means the lanes are not backing up and nothing is being batched. |
| `felix_broker_publish_worker_restarts_total` | counter | - | Publish executors replaced after a panic. Should stay 0; any increase is a bug worth reporting. |
| `felix_broker_acked_publishes_dropped_total` | counter | `reason` | Publishes the client was told succeeded because they were queued, which then could not be written, by `reason`. This is the loss path of acknowledging on enqueue. |
| `felix_broker_json_publishes_total` | counter | `frame` | Publishes that still arrived as JSON, by `frame`. Should be flat at zero; a rise means an older client is connected. |
| `felix_broker_publish_order_stalls_total` | counter | - | Control streams closed because a pipelined publish was never answered in order. Telemetry build only. |
| `felix_broker_publish_window_full_total` | counter | - | Publishes refused because the connection's window of unanswered publishes was full. Telemetry build only. |
| `felix_broker_ack_waiter_timeout_total` | counter | - | Commit-ack waiters that expired before the commit finished; the publish is answered with an error. Telemetry build only. |
| `felix_broker_ack_waiters_exhausted_total` | counter | - | Publishes refused because every commit-ack waiter slot was in use. Telemetry build only. |
| `felix_broker_out_ack_depth` | gauge | - | Acks queued for the connection writer, summed across connections. |
| `felix_broker_out_ack_full_total` | counter | - | Acks that found the connection's outgoing ack queue full. Telemetry build only. |
| `felix_broker_out_ack_timeout_total` | counter | - | Acks that timed out waiting for room in the outgoing ack queue. Telemetry build only. |
| `felix_broker_control_stream_closed_total` | counter | `reason` | Control streams closed because an ack could not be enqueued, by `reason`. Telemetry build only. |
| `felix_broker_control_stream_drain_ms` | histogram | - | Time a control stream spent draining its writer on close, in milliseconds. Telemetry build only. |
| `felix_broker_control_stream_drain_timeout_total` | counter | - | Control-stream drains that hit their deadline and aborted the writer. Telemetry build only. |

## Broker: ingress queue

The queue between the QUIC handlers and the publish workers, with its byte budgets. All of these are recorded only in a telemetry build.

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_broker_ingress_queue_depth` | gauge | - | Publish jobs waiting. Refreshed when the depth moves by 16 or more and when it empties. |
| `felix_broker_ingress_queue_full_total` | counter | - | Publishes that found the queue full. |
| `felix_broker_ingress_bytes_full_total` | counter | - | Publishes refused because the broker-wide ingress byte budget was exhausted. |
| `felix_broker_ingress_conn_bytes_full_total` | counter | - | Publishes refused because the connection's ingress byte budget was exhausted. |
| `felix_broker_ingress_dropped_total` | counter | - | Fire-and-forget publishes dropped because a budget or the queue was full. |
| `felix_broker_ingress_rejected_total` | counter | - | Publishes rejected back to the client because a budget or the queue was full. |
| `felix_broker_ingress_waited_total` | counter | - | Publishes that waited for room rather than being refused. |
| `felix_broker_ingress_parked_total` | counter | - | Publishes parked behind a full queue until a slot opened. |
| `felix_broker_ingress_backpressure_cancelled_total` | counter | - | Backpressured publishes cancelled because admission closed. |
| `felix_queue_depth_drift_total` | counter | `queue` | A depth gauge that would have gone negative and was clamped to zero, by `queue`. Should stay 0; a rise means an accounting bug. |

## Broker: subscribe and delivery

Per-subscriber queues, the writer lanes behind them and the replay ring. A rising `felix_sub_queue_dropped_total` is the signal that a subscriber is missing records.

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_subscribe_requests_total` | counter | `result` | Subscribe requests by result. Telemetry build only. |
| `felix_sub_queue_enqueued_total` | counter | - | Deliveries placed on subscriber queues. |
| `felix_sub_queue_dequeued_total` | counter | - | Deliveries taken off subscriber queues by the writer. |
| `felix_sub_queue_dropped_total` | counter | - | Records lost to slow subscribers. A subscriber that is not keeping up loses records instead of slowing the publisher; delivered events carry offsets so the gap is visible. |
| `felix_subscribe_dropped_total` | counter | - | Records dropped from subscriber queues; moves with `felix_sub_queue_dropped_total`. |
| `felix_sub_queue_byte_dropped_total` | counter | `bound` | Of the dropped records, those the byte bound turned away, by which `bound` was hit. |
| `felix_sub_queue_drop_old_emulated_total` | counter | - | Drops under a `DropOld` policy, which the broker implements as `DropNew`. |
| `felix_sub_queue_len` | gauge | - | Deliveries waiting across subscriber queues. |
| `felix_sub_queue_bytes` | gauge | - | Payload bytes waiting in subscriber queues. |
| `felix_sub_egress_bytes` | gauge | - | Payload bytes in the writer lanes, not yet written. Compare with `FELIX_SUB_EGRESS_MAX_BYTES`. |
| `felix_sub_lane_queue_len` | gauge | `lane` | Commands waiting in each writer lane. |
| `felix_sub_conn_queue_depth` | histogram | - | Commands waiting for a connection's writer, sampled per flush. |
| `felix_sub_active_connections` | gauge | - | Connections with at least one subscriber. |
| `felix_sub_connection_subscribers` | gauge | - | Subscribers across all connections. |
| `felix_sub_shared_batch_handles_total` | counter | - | Deliveries that shared one encoded batch between subscribers. |
| `felix_subscriber_disconnect_total` | counter | - | Subscribers disconnected by the writer, for example after a failed write. |
| `felix_subscription_lagged_total` | counter | - | `SubscriptionLagged` frames sent: a subscription lost records and was told where to resume. |
| `felix_broker_subscribe_conn_limit_rejected_total` | counter | - | Subscribes refused because the connection was at its subscription limit. Telemetry build only. |
| `felix_replay_ring_bytes` | gauge | - | Payload bytes held by every stream shard's replay ring. |

## Broker: connections and QUIC

Connection admission and protocol failures on the client listener.

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_quic_connections_refused_total` | counter | `reason` | Client connections refused on arrival, by `reason`. |
| `felix_quic_handshake_failures_total` | counter | - | Client handshakes that failed. |
| `felix_quic_auth_timeout_total` | counter | - | Connections closed for not authenticating in time. |
| `felix_quic_protocol_violation_total` | counter | `stream` | Streams closed for violating the protocol, by stream kind. Telemetry build only. |

## Broker: caches, queues and groups

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_cache_watch_lagged_total` | counter | - | Cache watches that fell behind and were marked lagged. The count includes watches that exceeded their byte bound. |
| `felix_cache_watch_queue_bytes` | gauge | - | Key and value bytes queued for cache watchers. |
| `felix_group_polls_capped_total` | counter | - | Group polls held short by `FELIX_GROUP_MAX_IN_FLIGHT`. Rising means a group has as many records in flight as it is allowed. |

## Broker: tenants

Per-tenant usage. The `tenant` label is capped; once the cap is reached further tenants share an overflow label.

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_tenant_published_messages_total` | counter | `tenant` | Messages a tenant published through this broker. |
| `felix_tenant_published_bytes_total` | counter | `tenant` | Payload bytes of those messages. |
| `felix_tenant_delivered_messages_total` | counter | `tenant` | Messages this broker handed to a tenant's subscribers. |
| `felix_tenant_delivered_bytes_total` | counter | `tenant` | Payload bytes of those messages. |
| `felix_tenant_publish_throttled_total` | counter | `tenant`, `action` | Publishes held back by the tenant's quota. `action` is `refused`, `dropped` or `delayed`. |
| `felix_tenant_publish_queue_full_total` | counter | `tenant`, `action` | Publishes the publish queue had no room for. `action` is `refused` or `dropped`. |
| `felix_tenant_metrics_overflow_total` | counter | - | Recordings that went to the overflow label because the tenant cap was reached. |

## Broker: lifecycle, credentials and revocation

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_ready_state` | gauge | - | `1` while the process is serving, `0` once it is draining. |
| `felix_inflight_requests` | gauge | - | Requests currently being served. |
| `felix_drain_duration_ms` | gauge | - | How long the last drain took, in milliseconds. |
| `felix_drain_forced_total` | counter | `subsystem` | Subsystems cancelled because the drain deadline expired. |
| `felix_broker_shutdown_handoffs_total` | counter | `outcome` | Shutdown handoffs by `outcome`: `completed`, `timed_out`, `interrupted` or `skipped`. |
| `felix_broker_shutdown_handoff_shards_total` | counter | - | Shards handed off during shutdown. |
| `felix_broker_shutdown_handoff_duration_ms` | gauge | - | How long the last handoff took, whatever its outcome. |
| `felix_broker_credential_expires_in_seconds` | gauge | - | Seconds until the node credential expires, or `-1` when it does not say. Alert on this; a broker with failing refreshes still looks healthy until the credential expires. |
| `felix_broker_credential_refreshes_total` | counter | `outcome` | Node-credential refreshes, `ok` or `unavailable`. |
| `felix_broker_credential_rotations_total` | counter | `outcome` | Node credentials adopted from a rotated token file, `ok` or `rejected`. |
| `felix_broker_principal_revocations_total` | counter | - | Principal revocations this broker applied. |
| `felix_revocation_feed_last_success_seconds` | gauge | - | Unix time of the last poll that reached the control plane's revocation feed. A stale value means revocations are not arriving. |
| `felix_revocation_feed_poll_errors_total` | counter | - | Revocation feed polls that failed. |
| `felix_broker_stream_incarnation_refused_total` | counter | - | Stream registrations refused because they came from an older catalog than the one the broker holds. |
| `felix_broker_cache_incarnation_refused_total` | counter | - | Cache registrations refused for the same reason. |
| `felix_broker_durable_stream_skipped_total` | counter | - | Durable streams skipped because the broker has no durable storage configured. |
| `felix_broker_durable_stream_failed_total` | counter | - | Durable streams whose storage failed to open while registering. |

## Broker: cluster membership, leases and shards

How a broker joins the cluster, holds its lease and follows shard assignments.

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_broker_membership_live` | gauge | - | `1` while the control plane considers this broker placeable. |
| `felix_broker_membership_registrations_total` | counter | `outcome` | Registration attempts: `registered`, `rejected` or `unavailable`. |
| `felix_broker_heartbeats_total` | counter | - | Heartbeats the control plane accepted. |
| `felix_broker_heartbeat_failures_total` | counter | `kind` | Heartbeats that did not land: `rejected` or `unavailable`. |
| `felix_broker_heartbeat_age_seconds` | gauge | - | Seconds since the last accepted heartbeat. Alert when it nears the control plane's expiry timeout. |
| `felix_broker_lease_held` | gauge | - | `1` while this broker may serve the shards it leads. |
| `felix_broker_lease_expiries_total` | counter | - | Times the lease lapsed. Each is a window in which this broker's shards were unavailable here and not yet reassigned. |
| `felix_broker_lease_refusals_total` | counter | `boundary` | Requests refused because the lease was invalid. `boundary` is `admission`, `commit` or `ack` for writes and `read` for reads. A non-zero `commit` count means requests were outliving the lease. |
| `felix_broker_node_catalog_nodes` | gauge | - | Nodes in the address book this broker can forward to. Zero while assignments are known means every remote shard resolves to owner unavailable. |
| `felix_broker_node_catalog_refresh_failures_total` | counter | - | Node-catalog refreshes that failed. The previous catalog is kept, so routes go stale rather than disappear. |
| `felix_broker_fleet_feature_enabled` | gauge | `feature` | `1` for each fleet feature this broker has enabled, `0` for one it reported that no operator has finalized yet. |
| `felix_broker_shard_assignments` | gauge | - | Assignments held after the last snapshot. |
| `felix_broker_shard_phase` | gauge | `phase` | Shards this broker holds, by phase. |
| `felix_broker_shard_transitions_total` | counter | `from`, `to` | Local ownership transitions, by direction. |
| `felix_broker_shard_open_failures_total` | counter | - | Local logs that could not be opened. Non-zero means a shard the cluster believes is placed here is not being served. |
| `felix_broker_shard_switchover_seconds` | histogram | - | Seconds from seeing the old leader fenced to serving the shard. Nobody serves the shard during this window. |
| `felix_broker_shard_move_seconds` | histogram | - | Seconds from first seeing itself named as a shard's destination to serving it, copy included. |
| `felix_broker_shard_move_held_total` | counter | - | Writes held for a moving shard. |
| `felix_broker_shard_move_hold_seconds` | histogram | - | Seconds each held write waited, however it ended. |
| `felix_broker_shard_move_hold_refused_total` | counter | `reason` | Writes to a moving shard refused: `timed_out` or `full`. |
| `felix_broker_shard_watch_checkpoint` | gauge | - | Checkpoint the assignment watch has reached. Compare with the control plane's `next_seq` to see lag. |
| `felix_broker_shard_watch_snapshots_total` | counter | - | Assignment snapshots taken. Rises at start and on every forced resync. |
| `felix_broker_shard_watch_resyncs_total` | counter | `reason` | Forced resyncs: `gap_in_history` or `sequence_reset`. |
| `felix_broker_shard_watch_failures_total` | counter | - | Watch polls that failed outright. |
| `felix_broker_shard_confirmed_reads_total` | counter | `outcome` | Confirmed assignment reads for a copy with no election promise: `assigned`, `unassigned`, `not_served` or `unavailable`. `not_served` means the control plane cannot confirm a read and the routing view decides alone. |
| `felix_broker_shard_changes_applied_total` | counter | - | Assignment changes applied. |
| `felix_broker_shard_changes_stale_total` | counter | - | Changes ignored for carrying a generation at or below the one held. Routine in small numbers; it is what makes duplicate delivery harmless. |
| `felix_broker_shard_stale_events_total` | counter | - | Events ignored for naming a generation at or below the one already acted on. |

## Broker: forwarding and peer transport

Broker-to-broker traffic: publishes forwarded to a shard's owner and the peer connections that carry them and replication.

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_broker_forwards_total` | counter | `outcome` | Publishes this broker forwarded to an owner, by outcome. |
| `felix_broker_forward_retries_total` | counter | - | Forward attempts after the first: a redirect followed or a retryable refusal retried. Rising steadily means the routing view is churning. |
| `felix_broker_peer_connections` | gauge | - | Live peer connections. |
| `felix_broker_peer_streams` | gauge | - | Multiplexed request streams open across all peers. |
| `felix_broker_peer_connect_attempts_total` | counter | `outcome` | Connection attempts: `connected`, `unreachable` or `handshake`. |
| `felix_broker_peer_connection_losses_total` | counter | `reason` | Connections that dropped after being established. |
| `felix_broker_peer_reconnects_total` | counter | - | Redials after a loss. |
| `felix_broker_peer_lanes_replaced_total` | counter | - | Request streams replaced because the peer stopped answering on them. |
| `felix_broker_peer_requests_total` | counter | `outcome` | Requests sent to peers. |
| `felix_broker_peer_requests_shed_total` | counter | - | Requests refused before sending because the peer was at its in-flight limit. Nothing was attempted. |
| `felix_broker_peer_request_seconds` | histogram | `outcome` | Round-trip latency of a request to a peer. |
| `felix_broker_peer_served_total` | counter | `outcome` | Requests this broker served for a peer. |
| `felix_broker_peer_inbound_rejected_total` | counter | `reason` | Inbound peer connections refused: `alpn` or `handshake`. |

## Replication

Replication from a shard's leader to its followers, and the `Quorum` publish path. See [Durable storage](/architecture/durable-storage/) for how the log underneath behaves.

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_broker_replication_shipped_total` | counter | `outcome` | Replication batches shipped as a leader. |
| `felix_broker_replication_lag_records` | gauge | - | How far the slowest follower is behind, in records, across every shard this broker leads. This is the loss window for `Leader` consistency. |
| `felix_broker_replication_halted` | gauge | - | Followers replication has stopped for because their log diverged or this broker was superseded. Any non-zero value needs attention. |
| `felix_broker_replication_rebuilding` | gauge | - | Followers being rebuilt right now. |
| `felix_broker_replication_rebuilds_total` | counter | `outcome` | Follower rebuilds by outcome. |
| `felix_broker_replication_move_throttled_bytes_total` | counter | - | Bytes shipped to a move's destination under `FELIX_SHARD_MOVE_BYTES_PER_SEC`. |
| `felix_broker_replication_drain_withheld_total` | counter | `log` | Drained reports held back because a follower was still behind on a log. A planned move waits on this. |
| `felix_broker_replicated_total` | counter | `outcome` | Replication batches stored as a follower. `gap` is ordinary during catch-up; other failure outcomes need different responses. |
| `felix_broker_replica_reports_per_request` | histogram | - | Shards' reports sharing one control-plane request. One means nothing was batched. |
| `felix_broker_quorum_marks_withheld_total` | counter | - | Quorum marks held back because the replica report did not reach the control plane. `Quorum` publishes time out on those shards while nothing else looks unwell. |
| `felix_broker_publish_quorum_failed_total` | counter | `reason` | `Quorum` publishes that did not reach a majority. |
| `felix_broker_publish_quorum_wait_seconds` | histogram | - | How long a `Quorum` publish waited for its majority, whatever the outcome. |
| `felix_broker_promotions_opened_total` | counter | `path` | Shards opened for writes at a new leadership. `path` is `fenced` once a majority took the fence and `lease` when some replica did not offer it. |
| `felix_broker_promotion_truncated_total` | counter | - | Times a promoted leader dropped records of its own that a replica's newer log had superseded. |
| `felix_broker_fence_refused_staged_successor_total` | counter | - | Fences a crashed leader's copy refused because the failover's fence ids also name a copy beside the set of two, as after a move's staged successor. The shard stays closed until `felixctl placement abandon` ([#1635](https://github.com/GetFelix/felix/issues/1635)). |

## Storage

The durable log. Every series is process-wide, not per stream. Compare `felix_storage_append_duration_seconds` with `felix_storage_sync_duration_seconds` to see whether the fsync policy is the cost, and read `felix_storage_sync_batch_appends` for group-commit fan-in. See [Durable storage](/architecture/durable-storage/).

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_storage_append_records_total` | counter | - | Records accepted into the log. |
| `felix_storage_append_bytes_total` | counter | - | Encoded bytes written, header included. |
| `felix_storage_append_batch_records` | histogram | - | Records per append batch. Batch size is the main lever on syscall cost. |
| `felix_storage_append_duration_seconds` | histogram | - | End-to-end time for an append, including any durability wait. |
| `felix_storage_sync_total` | counter | - | Device flushes performed. |
| `felix_storage_sync_duration_seconds` | histogram | - | Time inside a single flush. |
| `felix_storage_sync_batch_appends` | histogram | - | Records made durable by one `OnCommit` flush: the group-commit fan-in. Near 1 under concurrent load means appends are serializing on the device. |
| `felix_storage_sync_failures_total` | counter | - | Flushes that failed. Any non-zero value means acknowledged durability is in doubt. |
| `felix_storage_unsynced_bytes` | gauge | - | Bytes written but not yet flushed: what a crash would lose right now. |
| `felix_storage_full_total` | counter | - | Writes and file creations refused because the device or quota was full. |
| `felix_storage_index_write_failures_total` | counter | - | Sparse index writes that failed. Non-fatal, the index is rebuilt on open, but a persistent rise makes every restart pay a rebuild scan. |
| `felix_storage_replica_state_writes_total` | counter | - | Replica-state files written: a raised accepted generation, a commit offset or a retention hold. |
| `felix_storage_replica_state_write_duration_seconds` | histogram | - | Time to write a replica-state file. |
| `felix_storage_segment_roll_total` | counter | - | Segment rollovers. |
| `felix_storage_segment_roll_background_total` | counter | - | Rollovers started in the background, off the append path. |
| `felix_storage_segment_roll_failed_total` | counter | - | Background rollovers that failed and degraded the log. |
| `felix_storage_segment_roll_discarded_total` | counter | - | Prepared segments discarded because the tail moved on before the swap. |
| `felix_storage_segment_reserve_failed_total` | counter | - | Extensions of an active segment's block reservation that failed. Appends carry on and allocate as they write. |
| `felix_storage_segment_count` | gauge | - | Segments currently on disk for a shard. |
| `felix_storage_open_sealed_segments` | gauge | - | Sealed segments whose file and index are open, across every log. |
| `felix_storage_retention_segments_deleted_total` | counter | - | Sealed segments deleted by retention. |
| `felix_storage_retention_bytes_reclaimed_total` | counter | - | Bytes reclaimed by retention. |
| `felix_storage_retention_base_offset` | gauge | - | Oldest offset still readable. Rises only when retention deletes a segment. |
| `felix_storage_retention_failures_total` | counter | - | Retention passes that failed. The next pass retries, but a persistent rise means the log grows unbounded. |
| `felix_storage_offload_segments_total` | counter | - | Sealed segments copied to the object store, verified and recorded. |
| `felix_storage_offload_bytes_total` | counter | - | Bytes copied to the object store. |
| `felix_storage_offload_failures_total` | counter | - | Segment offloads that failed. The segment stays local and retention cannot delete it until a later pass succeeds. |
| `felix_storage_offload_failing_logs` | gauge | - | Offloaded logs whose last pass failed. Non-zero means local disk is growing past retention. |
| `felix_storage_offload_held_bytes` | gauge | - | Bytes retention would delete but keeps because they have no recorded copy yet. |
| `felix_storage_read_records_total` | counter | - | Records returned by range reads. |
| `felix_storage_read_bytes_total` | counter | - | Payload bytes returned by range reads. |
| `felix_storage_read_duration_seconds` | histogram | - | Time to serve a range read. |
| `felix_storage_recovery_duration_seconds` | histogram | - | Time spent validating segments at startup. |
| `felix_storage_recovery_truncated_bytes` | counter | - | Bytes discarded from a torn tail during recovery. |
| `felix_storage_recovery_abandoned_rolls_total` | counter | - | Empty segments removed at startup because a rollover never installed them. |
| `felix_storage_recovery_index_rebuilds_total` | counter | - | Indexes rebuilt because they were missing or stale. |
| `felix_storage_producer_state_rebuilt_total` | counter | - | Opens or truncations that rebuilt producer state from sealed segments because the snapshot was missing or out of date. |

## Kafka gateway

Served by the Kafka-compatible listener. See [Kafka compatibility](/features/kafka/).

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_kafka_connections` | gauge | - | Open Kafka connections. |
| `felix_kafka_connections_total` | counter | - | Kafka connections accepted. |
| `felix_kafka_requests_total` | counter | `api`, `error` | Kafka requests by API and error. |
| `felix_kafka_refused_total` | counter | `reason` | Connections or requests refused, by `reason`. |
| `felix_kafka_produce_records_total` | counter | - | Records produced. |
| `felix_kafka_produce_bytes_total` | counter | - | Bytes produced. |
| `felix_kafka_produce_duplicate_records_total` | counter | - | Produced records recognized as duplicates of an earlier retry. |
| `felix_kafka_produce_dropped_total` | counter | `field` | Produced record fields Felix does not keep, by `field` (`key` or `headers`). |
| `felix_kafka_produce_errors_total` | counter | `error` | Produce failures by error. |
| `felix_kafka_fetch_records_total` | counter | - | Records returned to fetches. |
| `felix_kafka_fetch_bytes_total` | counter | - | Bytes returned to fetches. |
| `felix_kafka_fetch_waits_total` | counter | `outcome` | Fetches that waited for data, by how the wait ended. |
| `felix_kafka_fetch_wait_seconds` | histogram | - | How long fetches waited for data. |

## Control plane: placement

Shard placement and moves, run by the instance holding the placement lease.

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_placement_lease_held` | gauge | - | `1` while this instance holds the placement lease. Across instances the sum is 1, or 0 for up to a lease period after the holder stops without releasing it. |
| `felix_placement_lease_takeovers_total` | counter | - | Times this instance took the placement lease. |
| `felix_placement_writes_fenced_total` | counter | - | Passes and operator requests not written because another placement write landed after they read the store. The next pass re-plans. |
| `felix_shard_assignment_write_conflicts_total` | counter | - | Placement writes that lost a compare-and-set against a concurrent write. |
| `felix_shard_assignment_changes_total` | counter | `op` | Shard assignment changes published to the changefeed, by `op`. |
| `felix_shards_placed_total` | counter | - | Shards assigned a leader. |
| `felix_shards_unplaceable` | gauge | - | Shards with no eligible leader right now. Non-zero means capacity or liveness needs attention. |
| `felix_shards_under_replicated` | gauge | - | Shards with fewer copies on serving brokers than their replication factor. |
| `felix_shards_zone_unspread` | gauge | - | Shards whose copies share a zone although live brokers in other zones could hold them. |
| `felix_shard_replicas_missing` | gauge | - | Copies that under-replicated shards are missing between them. |
| `felix_shard_replicas_halted` | gauge | - | Copies whose leader has stopped shipping to them. Placement keeps new copies off those nodes. |
| `felix_shard_moves_waiting` | gauge | - | Moves that could not advance in the last pass. |
| `felix_shard_moves_timed_out_total` | counter | - | Moves and follower replacements given up because they did not get close enough to the leader within the move timeout. |
| `felix_shard_move_steps_total` | counter | `step` | Move steps written, by step. |
| `felix_shard_move_duration_seconds` | histogram | - | Seconds from the first step of a move to its cut-over. |
| `felix_shard_move_fence_seconds` | histogram | - | Seconds from the fence to the cut-over: the window in which the shard is not served. |
| `felix_shard_reconcile_failures_total` | counter | - | Passes that could not read the catalog at all. |
| `felix_replica_status_rejected_total` | counter | `reason` | Replica status reports from brokers that were rejected. |

## Control plane: nodes and catalog

Broker membership and the stream, cache and tenant catalog.

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_node_count` | gauge | `lifecycle`, `region` | Registered brokers, by lifecycle and region. |
| `felix_node_registrations_total` | counter | `outcome` | Registration attempts: `new`, `restart` or `rejected`. |
| `felix_node_transitions_total` | counter | `from`, `to` | Lifecycle moves. `live -> down` is the failure signal. |
| `felix_node_expiry_total` | counter | - | Nodes moved to `down` by the expiry sweep. |
| `felix_node_expiry_failures_total` | counter | - | Expiry sweeps that failed outright. Non-zero means liveness is stale. |
| `felix_node_changes_total` | counter | `op` | Membership changes published to the changefeed, by `op`. |
| `felix_streams_total` | gauge | - | Streams in the catalog. |
| `felix_stream_changes_total` | counter | `op` | Stream catalog changes, by `op`. |
| `felix_caches_total` | gauge | - | Caches in the catalog. |
| `felix_cache_changes_total` | counter | `op` | Cache catalog changes, by `op`. |

## Control plane: authentication

Token issue, refresh and revocation on the control plane.

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_controlplane_auth_rejected_total` | counter | `reason` | Credential refusals, by `reason` from a closed set. |
| `felix_bootstrap_attempts_total` | counter | `outcome`, `reason` | Tenant bootstrap attempts. Bootstrap is presented once per tenant by an operator, so anything but the normal outcomes is a misconfigured deploy or someone probing. |
| `felix_delegated_tokens_issued_total` | counter | - | Delegated tokens minted. |
| `felix_refresh_tokens_issued_total` | counter | `via` | Refresh tokens issued, by how (`via`). |
| `felix_refresh_tokens_revoked_total` | counter | - | Refresh tokens revoked. |
| `felix_refresh_token_replays_total` | counter | - | Refresh tokens presented after being rotated away, outside the grace window. A leaked token is the usual cause. |
| `felix_refresh_token_grace_reuses_total` | counter | - | Refresh tokens reused inside the grace window, as after a lost response. |
| `felix_refresh_token_bad_secret_total` | counter | - | Refresh attempts with a wrong secret. |
| `felix_principal_revocations_total` | counter | - | Principals revoked through the admin API. |

## Control plane: metadata Raft

Present when the control plane runs its metadata Raft group.

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_meta_raft_is_leader` | gauge | - | `1` on the member that is currently leader. |
| `felix_meta_raft_leader_known` | gauge | - | `1` while this member knows who the leader is. |
| `felix_meta_raft_term` | gauge | - | Current Raft term. A term that keeps climbing means elections are not settling. |
| `felix_meta_raft_last_log_index` | gauge | - | Index of the last log entry. |
| `felix_meta_raft_last_applied_index` | gauge | - | Index of the last entry applied. A gap to the last log index is apply lag. |
| `felix_meta_raft_snapshot_index` | gauge | - | Index of the last snapshot. |
| `felix_meta_raft_member_below_floor` | gauge | `member` | `1` while replication to a Raft `member` is held back because its log is below the group's join floor, as after a wiped volume. Clears when the member catches up. |
| `felix_meta_raft_forwarded_proposals_total` | counter | - | Proposals a follower forwarded to the leader. Informational; forwarding is correct, one hop more. |
| `felix_meta_raft_deduplicated_proposals_total` | counter | - | Proposals answered from the applied record because a retry carried an id already applied. |
| `felix_meta_raft_write_timeouts_total` | counter | - | Proposals that ran out their whole budget, which means no leader or no quorum. Alert on this. |
| `felix_meta_raft_peer_rejected_total` | counter | `reason` | Raft peer requests refused, by `reason`. |
| `felix_meta_raft_unsupported_commands_total` | counter | - | Commands this member's build does not understand. In a mixed-version group the member's state now differs from a newer leader's. |

## Clients

Recorded in the process using the Rust client (and the bindings built on it), not on the broker. Metrics marked as telemetry only are recorded only in a client built with the `telemetry` feature.

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_client_sub_queue_enqueued_total` | counter | - | Frames placed on the client's subscribe queue. |
| `felix_client_sub_queue_dequeued_total` | counter | - | Frames taken off the client's subscribe queue. |
| `felix_client_sub_queue_dropped_total` | counter | - | Frames dropped because the client's subscribe queue was full. The application is not keeping up. |
| `felix_client_sub_queue_drop_old_emulated_total` | counter | - | Drops under a `DropOld` policy, which the client implements as `DropNew`. |
| `felix_client_sub_queue_len` | gauge | - | Frames waiting in the client's subscribe queue. |
| `felix_client_sub_dispatch_enqueued_total` | counter | - | Events placed on the dispatch queue to application callbacks. |
| `felix_client_sub_dispatch_dropped_total` | counter | - | Events dropped because the dispatch queue was full. |
| `felix_client_sub_dispatch_drop_old_emulated_total` | counter | - | Dispatch drops under a `DropOld` policy, implemented as `DropNew`. |
| `felix_client_publish_forwarded_total` | counter | `owner` | Publishes the broker had to relay to another broker, by `owner`. Telemetry only. A relayed publish costs roughly half the throughput per core; route to the owner instead. |
| `felix_client_publish_cancelled_after_enqueue_total` | counter | - | Publishes whose caller went away after the publish was queued. Telemetry only. |
| `felix_client_cache_conn_ops` | gauge | `conn` | Cache operations in flight on a connection. Telemetry only. |
| `felix_client_cache_conn_ops_total` | counter | `conn` | Cache operations sent on a connection. Telemetry only. |
| `felix_client_event_conn_subscriptions` | gauge | `conn` | Subscriptions on a connection. Telemetry only. |
| `felix_client_event_conn_subscriptions_total` | counter | `conn` | Subscriptions opened on a connection. Telemetry only. |

## Per-stage latency histograms (telemetry builds)

Stage timings for performance work, recorded in nanoseconds only when the broker or client is built with the `telemetry` feature. They answer where a request spent its time, not whether it is healthy. See [Per-stage telemetry](/features/observability/#per-stage-telemetry). The names without a `felix_` prefix are the same kind of series. Series labelled `connection_id` have one series per subscriber connection and are meant for a short test run, not for a long-lived production scrape.

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_broker_decode_ns` | histogram | - | Frame decode on the broker. |
| `felix_broker_ingress_enqueue_ns` | histogram | - | Placing a publish on the ingress queue. |
| `felix_broker_quic_write_ns` | histogram | - | Writing a response to the QUIC stream. |
| `felix_broker_sub_prefix_build_ns` | histogram | - | Building the per-subscriber frame prefix. |
| `felix_broker_sub_queue_wait_ns` | histogram | - | Waiting to enqueue to a subscriber connection's writer. |
| `felix_client_encode_ns` | histogram | - | Encoding a publish on the client. |
| `felix_client_write_ns` | histogram | - | Writing a publish to the connection. |
| `broker_publish_lookup_ns` | histogram | - | Stream lookup on a publish. |
| `broker_publish_send_ns` | histogram | - | Handing a publish to the stream. |
| `broker_publish_append_ns` | histogram | - | Appending a publish to an in-memory stream. |
| `broker_publish_durable_append_ns` | histogram | - | Appending a publish to the durable log. |
| `broker_publish_fanout_total_ns` | histogram | - | Fanning a publish out to subscribers. |
| `broker_sub_lane_enqueue_ns` | histogram | `lane` | Enqueueing to a subscriber writer lane. |
| `broker_sub_lane_enqueue_block_ns` | histogram | `lane` | Time blocked enqueueing to a full lane. |
| `broker_sub_lane_enqueued_total` | counter | `lane` | Commands enqueued to a lane. |
| `broker_sub_lane_queue_wait_ns` | histogram | `connection_id` | Time a command waited in a lane. |
| `broker_sub_conn_enqueue_wait_ns` | histogram | `connection_id` | Waiting to enqueue to a connection. |
| `broker_sub_conn_write_calls_total` | counter | `connection_id` | Write calls on a subscriber connection. |
| `broker_sub_conn_writes_per_flush` | histogram | `connection_id` | Writes coalesced per flush. |
| `broker_sub_conn_write_ns` | histogram | `connection_id` | Time to write to a subscriber connection. |
| `broker_sub_conn_write_await_ns` | histogram | `connection_id` | Same measurement as `broker_sub_conn_write_ns`, recorded under a second name. |
| `broker_sub_conn_write_errors_total` | counter | `connection_id` | Subscriber writes that failed and removed a subscriber. |
| `broker_sub_conn_bytes_written_total` | counter | `connection_id` | Bytes written to a subscriber connection. |
| `broker_sub_conn_avg_bytes_per_write` | histogram | `connection_id` | Bytes per write call on a subscriber connection. |
| `broker_sub_lane_dequeue_to_write_start_ns` | histogram | `connection_id` | Time from a command leaving a lane to its write starting. |
| `broker_sub_lane_queue_len_highwater` | counter | `lane` | Increments each time a lane's queue length passes its previous high-water mark, so the total is the high-water mark. |
| `broker_sub_time_to_first_dequeue_ns` | histogram | `connection_id` | Time from a connection's first enqueue to its first dequeue. |
| `broker_sub_stream_mode_fallback_total` | counter | `mode` | Subscribes that asked for the `hashed_pool` stream mode, which is not enabled yet, and got a stream per subscriber instead. |
| `broker_sub_write_blocked_ns` | histogram | - | Time a subscriber write was blocked. |
| `client_pub_enqueue_wait_ns` | histogram | - | Waiting to enqueue a publish. |
| `client_text_batch_build_ns` | histogram | - | Building a text batch. |
| `client_send_await_ns` | histogram | - | Awaiting a send to complete. |
| `client_ack_read_wait_ns` | histogram | - | Waiting to read an ack. |
| `client_ack_decode_ns` | histogram | - | Decoding an ack. |
| `client_sub_read_await_ns` | histogram | - | Awaiting a read on a subscription. |
| `client_sub_consumer_gap_ns` | histogram | - | Gap between consumer polls. |
| `client_sub_poll_gap_ns` | histogram | - | Gap between pipeline polls. |
| `client_sub_queue_wait_ns` | histogram | - | Time a frame waited in the subscribe queue. |
| `client_sub_time_in_queue_ns` | histogram | - | Same measurement as `client_sub_queue_wait_ns`, under its older name. |
| `sub_decode_ns` | histogram | - | Decoding a delivered frame on the client. |
| `sub_dispatch_ns` | histogram | - | Dispatching an event to the application. |
| `client_e2e_latency_ns` | histogram | - | Publish-to-delivery latency, when the benchmark timestamp is embedded. |

## Lock-wait histograms

Recorded in nanoseconds by the broker regardless of the telemetry feature, to catch contention on the hot path.

| Metric | Type | Labels | Measures |
| --- | --- | --- | --- |
| `felix_perf_log_lock_wait_ns` | histogram | - | Time spent waiting for a stream's log lock. |
| `felix_perf_topics_read_lock_wait_ns` | histogram | - | Time spent waiting for the stream table's read lock. |
