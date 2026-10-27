---
title: "Running in Production"
description: "Size a Felix cluster, pick its durability settings, and harden it before real traffic arrives."
---

This page is the order to think in before a cluster takes real traffic: how big
each broker has to be, what durability setting buys which guarantee, and what
to lock down. It links the deep pages and does not repeat them. Variable names
and defaults are in the [environment reference](/reference/environment-variables/),
which is authoritative when this page and it disagree.

Felix publishes few measured sizing numbers, because the right ones depend on
payload size, fanout and the device under the log. Where the docs have a
measurement, this page links it. Everywhere else it gives the formula and the
metric that tells you the input. Measure with your own traffic first, for
example with the `latency-demo` binary described in
[Performance](/features/performance/).

## Sizing a broker

### Memory

Everything that holds payload bytes in memory has a bound you can set. The
defaults favor throughput, and several are per subscriber or per stream shard,
so the worst case multiplies by a count you control. Set the broker-wide totals
in production, then size the memory limit above the sum.

| What holds memory | Worst case | Bound |
| --- | --- | --- |
| Subscriber queues | subscribers x `FELIX_SUB_QUEUE_MAX_BYTES` (128 MiB), plus connections x `FELIX_SUB_EGRESS_MAX_BYTES` (256 MiB) | `FELIX_SUB_QUEUE_MAX_BYTES_TOTAL` caps all of it, writer lanes included. Unset by default. Entry count per queue: `FELIX_SUBSCRIBER_QUEUE_CAPACITY`, with a subscriber's own request capped at `FELIX_SUBSCRIBER_QUEUE_CAPACITY_MAX`. |
| Replay rings | stream shards served x `FELIX_REPLAY_RING_MAX_BYTES` (64 MiB), on top of 1024 records each | `FELIX_REPLAY_RING_MAX_BYTES_TOTAL`, unset by default. A ring that gives up records trades replay depth for memory and never affects live delivery. |
| Cache key index | one entry per live key, in memory, for every log-backed cache shard on the broker | None. It is rebuilt by replaying the log at open, so the key count of a cache is its memory cost. |
| Cache watch queues | watches x `FELIX_CACHE_WATCH_MAX_BYTES` | `FELIX_CACHE_WATCH_MAX_BYTES_TOTAL` |
| Publish in flight | `FELIX_BROKER_PUBLISH_INFLIGHT_BYTES` (64 MiB), shared by every connection | Per identity on a connection: `FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES` (16 MiB). Job count: `FELIX_BROKER_PUB_QUEUE_DEPTH` times `FELIX_BROKER_PUB_WORKERS_PER_CONN`. |
| Connections | connections x `FELIX_BROKER_PUB_CONN_RECV_WINDOW` of flow-control credit a client may park unread | `FELIX_MAX_CLIENT_CONNECTIONS` (8192), `FELIX_MAX_CONNECTIONS_PER_IP` (512), `FELIX_MAX_SUBSCRIPTIONS_PER_CONN` |

The cache key index has no published per-key cost. Measure it: load a
representative number of keys into a log-backed cache on a test broker, restart
it, and compare resident memory before and after. Divide by the key count and
multiply by your expected total, then add the headroom for the rest of the
table.

Connections follow from your clients, not from the broker. Each client holds
the pools its configuration names (`FELIX_PUB_CONN_POOL`, `FELIX_EVENT_CONN_POOL`,
`FELIX_CACHE_CONN_POOL`, `FELIX_CLUSTER_CONN_POOL`), and a cluster client dials
the owner of each shard it uses. Count clients times pool sizes against
`FELIX_MAX_CLIENT_CONNECTIONS`, and divide by the number of brokers the clients
spread over.

Watch these to see how close the cluster runs to its bounds:
`felix_sub_queue_bytes`, `felix_sub_egress_bytes`, `felix_replay_ring_bytes`,
`felix_cache_watch_queue_bytes` and `felix_broker_ingress_queue_depth`. A bound
that fires shows up as `felix_sub_queue_byte_dropped_total` or
`felix_tenant_publish_queue_full_total`, never as an out-of-memory kill, which
is the point of setting them. The [Felix dashboard](/features/observability/#dashboard)
has a panel for each.

### Disk

A shard's log on one broker settles near its retention bound, plus the active
segment:

```text
shard log on disk  ~  retention bound + FELIX_DURABLE_SEGMENT_BYTES
retention bound    =  FELIX_DURABLE_RETENTION_BYTES,
                      or ingest bytes/s of the shard x FELIX_DURABLE_RETENTION_SECONDS
broker disk        ~  sum over shards on the broker of (shard log on disk)
shards on a broker ~  stream shards x replication factor / brokers
```

Take the ingest figure from `rate(felix_storage_append_bytes_total[5m])`. It
counts encoded bytes with record headers, so it is the right input and
payload byte rates understate it. The active segment is never deleted, so a
log never settles below `FELIX_DURABLE_SEGMENT_BYTES` (256 MiB by default),
which matters when you have many small shards. Retention runs every
`FELIX_DURABLE_RETENTION_INTERVAL_SECONDS` (60), so usage overshoots between
passes.

Leave room for three things the formula leaves out:

- **A Quorum follower that is down.** Retention keeps records a majority has
  not acknowledged, so a shard grows past its bound until the follower returns.
- **Copies in flight.** A move, a restore or a drain copies a whole shard log
  to another broker. `FELIX_SHARD_MOVES_MAX_CONCURRENT` (1) bounds how many run
  at once; keep that much free space on every broker.
- **Failing offload.** With `FELIX_DURABLE_OFFLOAD_DIR`, retention cannot delete
  a segment until its archived copy is read back. `felix_storage_offload_held_bytes`
  is the space this is costing. See
  [Local disk is filling while offload is on](/deployment/diagnosing/#local-disk-is-filling-while-offload-is-on).

Log-backed caches compact rather than evict, at up to
`FELIX_STORAGE_COMPACTION_BYTES_PER_SEC`. The other storage knobs are under
[Durable storage](/reference/environment-variables/#durable-storage-configuration).

### CPU and network

- **Fanout.** A publish is encoded once and the frame shared across
  subscribers, so encoding cost follows the publish rate, not publish rate
  times subscribers. Writing the frames still follows delivered messages.
  Compare `felix_tenant_published_messages_total` with
  `felix_tenant_delivered_messages_total` for your real ratio.
- **TLS.** QUIC with TLS 1.3 is always on, so every payload byte pays for
  encryption on the way in and once per subscriber on the way out.
- **Parallelism.** One QUIC endpoint's driver is a single task, so
  `FELIX_QUIC_LISTENERS` (derived from cores, at most 4) is what lets a broker
  use more than one core for the transport. `FELIX_CORE_SHARDS` (off by default)
  pins stream work to cores and pays off with many streams; one stream always
  runs on one shard.
- **Network out of a leader** is about the delivered payload bytes plus
  (replication factor minus one) times the published bytes, since the leader
  ships every record to each follower. Size the NIC for that, not for the
  publish rate alone.

The measured single-broker and three-broker results, with their hardware, are
in [Benchmarks](/features/benchmarks/) and
[v0.6.0 performance](/features/v060-performance/#three-broker-cluster). The
hardware bands in [Performance](/features/performance/#sizing-guidelines) are
labelled illustrative there and are starting points, not ceilings.

## Choosing durability

Four settings decide what an acknowledged publish survives: the stream's
`durable` flag, the fsync mode on each broker, the stream's `consistency`, and
its `replication_factor`.

### Fsync mode

`FELIX_DURABLE_FSYNC_MODE` sets when bytes reach the device
([details](/reference/environment-variables/#felix_durable_fsync_mode)).

| Mode | An ack means | Loses on a machine crash or power loss |
| --- | --- | --- |
| `none` | In the page cache | Unbounded. A process crash is survived. |
| `periodic` (default) | In the page cache | Up to `FELIX_DURABLE_FSYNC_INTERVAL_MS` (250 ms) |
| `on_commit` | On the device | Nothing acknowledged |

`on_commit` costs one device flush per commit, about 4 ms on typical NVMe by
the environment reference's figure, shared by concurrent publishers through
group commit. Check that sharing works with the group-commit panel on the
dashboard: `felix_storage_sync_batch_appends` near 1 under concurrent publishers
means appends are not coalescing. Under `periodic` and `none`, a broker that
crashes restarts under a new storage id and its leaders copy each shard it
follows again.

### Leader or Quorum

| | `Leader` (default) | `Quorum` |
| --- | --- | --- |
| Acknowledged once | The leader has it | A majority of the shard's replicas, the leader included, has it |
| Leader dies with followers behind | No follower is promoted, because that would drop an acknowledged record. The shard is unavailable until the old leader returns with its disk. | A follower holding every acknowledged record is promoted. |
| Cost | Lowest latency | A replication round trip per commit. A wait past `FELIX_PUBLISH_QUORUM_TIMEOUT_MS` (5 s) is refused. |
| Failure shows up as | Failover time | Publish time |

Neither loses data silently. `Leader` trades availability for latency; see the
[Leader vs Quorum demo](https://github.com/GetFelix/felix/blob/main/docs/demos.md#leader-vs-quorum-felix-cluster-consistency).
A refused `Quorum` publish means "cannot be vouched for", and the record may
still be present, so check before you retry if a duplicate matters.

### Replication factor and zones

Quorum is a majority of the replication factor (RF). RF 3 tolerates one broker
down and keeps writing; RF 5 tolerates two. RF 2 gains availability from
nothing on `Quorum`, since its majority is both copies. RF 1 is a single copy
whose disk is the only one: a lost volume loses the shard.

Set `FELIX_NODE_ZONE` on every broker (the chart does this per StatefulSet with
`broker.zones`, see [Zones](/deployment/kubernetes/#zones)) so placement puts a
shard's copies in different failure domains. Run at least RF zones, or a zone
outage can take a majority. `felix_shards_zone_unspread` is non-zero while a
shard keeps copies in one zone for lack of room. Keep
`FELIX_SHARD_RESTORE_AFTER_MS` (5 minutes) above your longest broker restart,
so a rolling upgrade does not trigger copies.

### Picking a combination

| Need | Stream | Fsync | Notes |
| --- | --- | --- | --- |
| Fan-out where a lost message is acceptable | In-memory (not durable) | n/a | No disk, no replication. Subscribers see drops only through the counters. |
| Survive a broker process restart | Durable, RF 1 | `periodic` | A machine or disk loss loses the shard. |
| Survive a machine crash on one broker | Durable, RF 1 | `on_commit` | Still lost with the volume. |
| Survive a broker or volume loss with no acknowledged record lost | Durable, `Quorum`, RF 3 across 3 zones | `on_commit` on every broker | An ack then means fsynced on at least two brokers. |
| Lowest latency, tolerate a pause when a leader's disk is lost | Durable, `Leader`, RF 3 | `periodic` or `on_commit` | Failover waits for the old leader or its disk. |

`periodic` with `Quorum` is a middle choice: a majority holds the record in
page cache, so only a correlated machine crash across a majority loses it,
which zones make unlikely. `FELIX_ACK_ON_COMMIT=0` acknowledges before the
write and can lose an acknowledged record on a crash; leave it unset.

## Hardening checklist

Work through this before exposing a cluster. Each line names the setting and
points at where it is explained.

**Transport**

- [ ] A real broker certificate: `FELIX_TLS_CERT` and `FELIX_TLS_KEY`, with
      `FELIX_TLS_REQUIRE_CERT=true` so a broker without one refuses to start. The
      default is a self-signed certificate regenerated at every start.
      [Transport security](/features/security/#transport-security)
- [ ] `FELIX_TLS_REQUIRE_ALPN=true`. Add `FELIX_TLS_CLIENT_CA` for client
      certificates only if every client can present one; the Python,
      TypeScript, C and C# clients cannot yet.
- [ ] Peer mTLS between brokers: `FELIX_INTERNAL_TLS_CERT`, `_KEY` and `_CA`.
      Never `FELIX_INTERNAL_ALLOW_UNAUTHENTICATED` without a network policy that
      admits brokers alone.
- [ ] TLS on the control plane (`FELIX_CONTROLPLANE_TLS_CERT` and `_KEY`), brokers
      pointed at an `https://` `FELIX_CONTROLPLANE_URL`, plus
      `FELIX_CONTROLPLANE_CA` for a private CA. Node credentials and token
      exchange cross this link.
- [ ] Under the Raft backend, `FELIX_RAFT_TLS_*` and a strong
      `FELIX_RAFT_PEER_TOKEN`.

**Identity and access**

- [ ] Per-tenant identity providers registered with explicit audiences, and
      `oidc_allowed_algorithms` narrowed to what your provider signs with.
      [Authentication](/features/security/#authentication-and-authorization)
- [ ] Bootstrap used once, then `FELIX_BOOTSTRAP_ENABLED` turned off. While it is
      on, bind it to loopback and put mTLS on it (`FELIX_BOOTSTRAP_TLS_*`).
      `FELIX_BOOTSTRAP_DEV_TOKENS` is never set outside a local stack.
      [Bootstrap](/features/security/#bootstrap-mode-day-0)
- [ ] RBAC with least privilege: publishers get `stream.publish` on their
      streams, readers get `stream.subscribe`, and `tenant.manage` goes to
      few people.
      [RBAC](/features/security/#rbac-model-casbin)
- [ ] Revocation works end to end: alert on a stale revocation feed (the
      shipped alerts do), and know that a revoked token ends its open streams.
      [Revocation](/features/security/#revocation)

**Network exposure**

| Port | Who may reach it |
| --- | --- |
| Client QUIC, UDP `5000` and consecutive listeners | Client networks only |
| Internal QUIC, UDP `5001` | Other brokers only |
| Broker metrics, TCP `8080` | Prometheus and operators. Plain HTTP, no authentication, names tenants and streams. |
| Control-plane REST, TCP `8443` | Brokers and operators |
| Control-plane Raft peer, TCP `8444` | The other control-plane members |
| Bootstrap, TCP `9095` | Loopback or a port-forward, only while enabled |

The chart's NetworkPolicy enforces this when the cluster's CNI does; see
[Kubernetes](/deployment/kubernetes/). Do not publish `8080` or `9095` on a
load balancer.

**Limits and capacity**

- [ ] Tenant quotas: `FELIX_TENANT_PUBLISH_BYTES_PER_SEC`,
      `FELIX_TENANT_PUBLISH_MSGS_PER_SEC` or `FELIX_TENANT_PUBLISH_QUOTAS`. They
      apply per broker, so a tenant publishing through three brokers gets three
      times the number.
      [Quotas](/reference/environment-variables/#connection-limits-and-tenant-quotas)
- [ ] Connection limits set from your client count (see Memory above), and
      the broker-wide byte totals `FELIX_SUB_QUEUE_MAX_BYTES_TOTAL` and
      `FELIX_REPLAY_RING_MAX_BYTES_TOTAL`.
- [ ] Container memory limit above the worst case you computed. The chart's
      defaults (`broker.resources`) are a floor for a small cluster, not a
      recommendation for yours. Disk volumes sized with the Disk section.
- [ ] Retention set on every durable stream (`FELIX_DURABLE_RETENTION_BYTES`
      or `_SECONDS`, or the stream's own `retention`). Unset means unbounded.

**Availability and operations**

- [ ] A highly available control plane: three or five Raft members, or two or
      more instances over a highly available Postgres.
      [Control-plane HA](/deployment/control-plane-ha/)
- [ ] An odd number of brokers at least as large as RF, spread over at least RF
      zones, with PodDisruptionBudgets left at one broker at a time.
- [ ] Backups taken as backup points, and a restore rehearsed.
      [Backup and restore](/deployment/backup-and-restore/)
- [ ] An upgrade procedure that follows the compatibility rules, and fleet
      features finalized deliberately.
      [Upgrades](/deployment/upgrades/)
- [ ] Graceful shutdown deadlines that fit the pod grace period.
      [Graceful shutdown](/deployment/graceful-shutdown/)
- [ ] The dashboard imported and the alert rules loaded, with a route to
      whoever is on call. [Dashboard and alert rules](/features/observability/#dashboard-and-alert-rules)
- [ ] `felixctl` and the control-plane admin CLI tried against the cluster
      before the first incident.
      [Diagnosing a cluster](/deployment/diagnosing/)
