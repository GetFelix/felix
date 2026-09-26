---
title: "Observability"
---

Three windows into a running Felix: structured logs, Prometheus metrics, and
optional per-stage telemetry for performance work. This page lists what
actually exists and which signals answer which questions — every metric named
here is one the code emits.

## Logging

Felix logs through `tracing`, filtered by the standard `RUST_LOG` variable:

```bash
RUST_LOG=info                                  # default
RUST_LOG=felix_broker=debug                    # one module, louder
RUST_LOG=felix_broker=trace,felix_wire=debug   # several modules
```

Log lines are structured key-value events (tenant, stream, subscription id,
error), so they grep and parse cleanly. There is no JSON output mode today;
if your aggregation pipeline needs JSON, wrap the process output.

The lines worth knowing on sight:

- Startup: the QUIC listen address, and whether control-plane sync is on.
- `subscriber falling behind` / `events dropped for subscriber` — a
  subscription hit its bounded queue. This is the log-side view of
  `felix_sub_queue_dropped_total`.
- Drain lines during shutdown, saying which subsystems finished in time.

## Metrics

The broker and the control plane each serve Prometheus text on their own
metrics endpoint (`metrics_bind` on the broker; `/metrics`, plus `/live` and
`/ready` for probes).

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'felix-broker'
    static_configs:
      - targets: ['broker-1:8080', 'broker-2:8080', 'broker-3:8080']
```

Rather than an exhaustive list, here are the questions that come up and the
metrics that answer them.

**Is the publish path healthy?**

```prometheus
felix_publish_requests_total                # by result; a batch is one request
felix_publish_bytes_total                   # payload bytes of ok/accepted requests
felix_publish_latency_ms                    # histogram (telemetry)
felix_broker_ingress_queue_depth            # publish jobs waiting (telemetry)
felix_broker_ingress_dropped_total          # overflow, by policy (telemetry)
felix_broker_ingress_rejected_total         # (telemetry)
felix_broker_acked_publishes_dropped_total  # by reason: acked on enqueue, then not written
felix_broker_publish_worker_restarts_total  # publish workers replaced after a panic; should stay 0
felix_client_publish_forwarded_total        # client: publishes the broker had to relay (telemetry)
felix_broker_json_publishes_total            # by frame: publishes still on JSON
felix_client_publish_cancelled_after_enqueue_total  # client: publishes whose caller went away (telemetry)
```

Metrics marked *(telemetry)* are only recorded when the broker or client is
built with the `telemetry` feature (see [Per-stage telemetry](#per-stage-telemetry));
a default build does not export them. Everything else here is always on.

`felix_publish_requests_total` counts requests, not records: a batch of 64 is
one request. `result` is `accepted` for a fire-and-forget publish that reached
the ingress queue, `ok` for one that was acknowledged, and `dropped`, `error`,
`not_owner` or `unroutable` when it went nowhere. `forwarded` is counted on top
of the eventual outcome, when a broker relays a publish to the shard's owner.
`felix_publish_bytes_total` adds the payload bytes of every `accepted` and `ok`
request — payloads only, not frame or routing overhead.

`felix_client_publish_cancelled_after_enqueue_total` is a client metric, and
non-zero is not an error. Cancelling a publish after it reaches the worker does
not cancel the publish — the record is sent and very likely lands, and only the
caller learning so is lost. It is here because those records have to be
explicable: a timeout around a publish means *do not know*, not *did not
happen*, and this is the number that says how often that happened.

A rising ingress depth means publishers are outrunning the broker; drops and
rejections say the overflow policy fired, which is deliberate and visible.

`felix_client_publish_forwarded_total` is a client metric, labelled by the
owner the batch went to. Non-zero means this client is publishing to a broker
that does not own the shard, and each of those records is decrypted,
re-encrypted and decrypted again on the way — roughly half the throughput per
core. It is the client-side half of `felix_broker_forwards_total`, and the one
that says *which* client is mis-aimed.

`felix_broker_json_publishes_total` should be flat at zero. The data path is
binary; a client only falls back to JSON against a broker that did not advertise
the frame it wanted, so a non-zero rate means something in the deployment is
older than it looks — and it is paying for it, at roughly 70% of the binary
path's throughput.

**Are subscribers keeping up?**

```prometheus
felix_subscribe_requests_total              # (telemetry)
felix_sub_queue_enqueued_total
felix_sub_queue_dropped_total               # records lost to slow consumers
felix_sub_queue_drop_old_emulated_total     # DropOld configured, DropNew behavior
felix_sub_queue_len
felix_subscriber_disconnect_total
```

`felix_sub_queue_dropped_total` increasing is the signal that a subscriber is
missing records. On a durable stream the subscriber can detect this itself
from offset gaps and resume; on an ephemeral stream this counter is the only
witness.

**Is durability the bottleneck?** The storage layer's metrics are designed
around exactly this question — compare append time against sync time, and
watch the group-commit fan-in:

```prometheus
felix_storage_append_duration_seconds
felix_storage_sync_duration_seconds
felix_storage_sync_batch_appends       # appends served per device flush
felix_storage_unsynced_bytes           # what a crash would lose right now
felix_storage_sync_failures_total      # non-zero: acknowledged durability in doubt
```

If sync dominates append, the fsync policy is the cost. A
`sync_batch_appends` near 1 under concurrent load means appends are
serializing on the device instead of sharing a flush.

**Is the cluster healthy?** Membership from both sides, replication, and
leases:

```prometheus
felix_node_count                            # control plane: fleet size by lifecycle
felix_broker_membership_live                # broker: does the cluster still count me
felix_broker_heartbeat_age_seconds          # alert when this nears the expiry timeout
felix_broker_replication_lag_records
felix_broker_replication_halted             # a count; GET /replication/halted says which
felix_broker_replication_rebuilding         # halted followers the leader is rebuilding right now
felix_broker_replication_rebuilds_total     # by outcome: started, completed, refused
felix_broker_replication_drain_withheld_total # by log; a planned move waiting to hand over group state or counters
felix_broker_replica_reports_per_request    # shards per control-plane report; 1 on a busy broker means batching found nothing
felix_broker_lease_held
felix_broker_lease_refusals_total           # writes refused after a lease lapsed
felix_broker_credential_expires_in_seconds  # counts down; -1 when the token carries no exp
felix_broker_credential_refreshes_total     # by outcome: ok, unavailable
felix_broker_credential_rotations_total     # by outcome: ok, rejected — a token file rewritten from outside
```

**Are Kafka clients being served?** Only when the Kafka listener is on
(`FELIX_KAFKA_LISTEN`; see [Kafka compatibility](/felix/features/kafka/)):

```prometheus
felix_kafka_connections                     # gauge: Kafka connections open now
felix_kafka_connections_total
felix_kafka_requests_total                  # by api and error (none, not_leader_or_follower, ...)
felix_kafka_refused_total                   # by reason: unknown_api, unsupported_api, frame_size, connection_limit
felix_kafka_fetch_records_total
felix_kafka_fetch_bytes_total
felix_kafka_fetch_waits_total               # long polls, by outcome: data, timeout
felix_kafka_fetch_wait_seconds              # histogram: how long those polls waited
felix_kafka_produce_records_total           # records Kafka producers wrote
felix_kafka_produce_bytes_total             # their payload bytes
felix_kafka_produce_duplicate_records_total # re-sent idempotent records answered without writing
felix_kafka_produce_errors_total            # refused partitions, by error
felix_kafka_produce_dropped_total           # records whose key or headers were dropped, by field
```

`felix_kafka_produce_duplicate_records_total` rising is idempotence doing its
job: producers re-sending batches whose answers they lost, usually around a
failover or a move. `produce_errors_total{error="out_of_order_sequence_number"}`
should stay at zero; it means a producer believes records were written that the
log does not have. `produce_dropped_total{field="headers"}` shows producers
whose headers Felix cannot keep.

A steady `not_leader_or_follower` rate means clients keep fetching from a
broker that no longer leads the partition, which is normal for a moment after
a shard moves and a stale Metadata view if it lasts. `group_authorization_failed`
counts consumers that tried to join a group; Felix refuses those on purpose.

**Alert on `felix_broker_credential_expires_in_seconds` crossing a threshold**,
not only on the refresh and rotation counters. The counters say renewal is
failing; the gauge says how long that has left to matter. It matters a lot: the
heartbeat carries this credential and the heartbeat *is* the lease renewal, so a
token that expires is not a degraded broker — it is one that stops serving the
shards it leads once the lease lapses. That is the safe outcome, and still an
outage.

A broker joining a cluster refuses to start when the credential expires and
neither `FELIX_NODE_REFRESH_TOKEN_FILE` nor `FELIX_NODE_TOKEN_FILE` is set, so
the commonest way to reach that state is caught at rollout rather than an hour
in.

### Which replica stopped

`felix_broker_replication_halted` is a count, and stays one: a label per shard
would be a label per stream per tenant. A leader rebuilds a halted follower on
its own, one at a time by default (`FELIX_REPLICATION_REBUILD_MAX_CONCURRENT`),
so a brief non-zero is a rebuild queue. When it stays above zero, ask the broker
which:

```bash
curl -s http://broker:9090/replication/halted | jq
```

```json
[
  {
    "tenant_id": "t1",
    "namespace": "ns",
    "stream": "orders",
    "shard": 3,
    "kind": "stream",
    "node_id": "broker-b",
    "generation": 7,
    "next_offset": 120,
    "reason": "diverged",
    "remedy": "the follower holds different bytes at an offset this leader also holds, …"
  }
]
```

A halt does not clear on its own — that replica is out of every quorum until
someone acts — so any non-zero count is worth waking for. `reason` is stable
and safe to key a runbook off; `remedy` is prose and says whether the
follower's data is wrong or merely incomplete, which is what decides whether a
rebuild is the right move.

A healthy broker answers `[]`, not 404.

### Bootstrap attempts

`felix_bootstrap_attempts_total{outcome,reason}` covers the day-0 credential.
Bootstrap is presented once per tenant, by an operator, and never again — so a
*rejected* attempt is either a misconfigured deploy or someone guessing, and a
burst of `already_initialized` refusals against live tenants is what a leaked
token looks like.

```promql
# Anything but the two normal outcomes is worth waking for.
rate(felix_bootstrap_attempts_total{outcome="rejected"}[5m])

# A leaked token being tried against tenants that already exist.
rate(felix_bootstrap_attempts_total{reason="already_initialized"}[5m])
```

`reason` is a small closed set — `missing_token`, `malformed_token`,
`invalid_token`, `no_token_configured`, `already_initialized`, `ok` — so it is
safe to group by. The token itself is never logged, including a near miss.

**Example queries**:

```promql
# Publish rate
rate(felix_publish_requests_total[1m])

# p99 publish latency
histogram_quantile(0.99, rate(felix_publish_latency_ms_bucket[5m]))

# Records lost to slow consumers
rate(felix_sub_queue_dropped_total[5m])

# Group-commit effectiveness
rate(felix_storage_sync_batch_appends_sum[5m]) / rate(felix_storage_sync_batch_appends_count[5m])
```

## Shutdown and drain

Four signals, and the reason each one exists.

| Metric | Type | What it tells you |
| --- | --- | --- |
| `felix_ready_state` | gauge | `1` while serving, `0` once draining. Distinguishes an instance that left rotation deliberately from one that vanished. |
| `felix_inflight_requests` | gauge | Requests being served right now. Watch it fall to zero during a drain. |
| `felix_drain_duration_ms` | gauge | How long the last drain took. |
| `felix_drain_forced_total` | counter, by `subsystem` | Subsystems cancelled because the deadline expired. **Non-zero means work was dropped.** |

The last one is the point. A drain that finished in time and a drain that was
cut off both take roughly the deadline to report, so duration alone cannot tell
them apart — and the log line that says which does not survive the pod.

Alert on `felix_drain_forced_total` increasing. Everything else here is for
watching a rolling restart happen.

## Probes

The metrics listener serves `/live` and `/ready`, and they answer different
questions on purpose. `/live` says "this process can respond at all" and
touches nothing outside the process — a liveness probe drives restarts, and
restarting every instance because a dependency is down turns one outage into
a restart loop. `/ready` says "send this instance traffic," and goes false
first thing during shutdown so load balancers steer away before anything
stops working. See [Graceful shutdown](/felix/deployment/graceful-shutdown/).

## Distributed tracing

Tracing export is off unless an OTLP endpoint is set. With
`OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`) set,
the broker and control plane build an OTLP tracer provider on startup and
install a `tracing-opentelemetry` layer; without either, they log locally and
export nothing. An unreachable collector does not stop a broker from serving:
failed exports are logged by the exporter and the spans are dropped.

**Configuration** is by environment variable. Felix does take a YAML config file
(`FELIX_BROKER_CONFIG`, see [Configuration](/felix/reference/configuration/)),
but it has no tracing keys — the exporter speaks OTLP over gRPC (tonic) and is
configured entirely through the standard OTel variables:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
export RUST_LOG=info                      # the tracing subscriber's filter
```

Resource attributes are attached from the environment, so a span carries where
it came from without the broker being told twice:

| Variable | Becomes |
|---|---|
| `FELIX_SERVICE_INSTANCE_ID`, falling back to `HOSTNAME` | `service.instance.id` |
| `K8S_CLUSTER_NAME` | `k8s.cluster.name` |
| `K8S_NAMESPACE_NAME` | `k8s.namespace.name` |
| `K8S_POD_NAME` | `k8s.pod.name` |
| `CLOUD_REGION` | `cloud.region` |
| `DEPLOYMENT_ENVIRONMENT` | `deployment.environment` |

## Per-stage telemetry

For performance investigations, both the broker and client can record
per-stage timing samples — decode, fanout, write, and so on. It is off by
default and behind a feature flag, because it is a profiling tool, not a
production metrics system. The same flag turns on the hot-path metrics marked
*(telemetry)* above:

```toml
[dependencies]
felix-client = { version = "0.1", features = ["telemetry"] }
```

On the client, `felix_client::frame_counters_snapshot()` returns frame-level
counters, and `felix_client::timings::take_samples()` drains the recorded
per-stage samples. The benchmarks and the `latency-demo` binary are the
worked examples of reading them.

On the broker, `FELIX_CONN_STATS_MS` logs QUIC path statistics (MTU, cwnd,
RTT, loss, flow-control blocking) for healthy connections on an interval —
the data that says whether a throughput problem is transport-side or above
it. Off unless set.

## Debugging quick answers

- **Subscribers receive nothing**: check the subscription was created (log
  line), the stream exists, and the application is actually awaiting
  `next_event()`. Then check `felix_sub_queue_dropped_total`.
- **Publish latency spiked**: check `felix_broker_ingress_queue_depth`
  (broker backed up), then `felix_storage_sync_duration_seconds` (durability
  is the cost), then the client-side telemetry to see which stage grew.
- **Cache misses you didn't expect**: TTL expiry, a broker restart on an
  ephemeral cache, or a key/scope mismatch — in that order of likelihood.
