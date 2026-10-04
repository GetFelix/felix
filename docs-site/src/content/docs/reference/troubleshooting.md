---
title: "Troubleshooting Guide"
---

The failures people hit, what each one means, and how to confirm it. Log lines
and errors are quoted as the broker prints them.

## The broker will not start

Run `felix-broker --print-config` with the same environment and config file.
It loads the configuration exactly as startup does and exits without binding
anything, so most of the refusals below show up there first. See
[Seeing what is in effect](/felix/reference/environment-variables/#seeing-what-is-in-effect).

### No control-plane URL

```
Error: FELIX_CONTROLPLANE_URL must be set for auth
```

Every broker needs a control plane, a single broker included. Set
`FELIX_CONTROLPLANE_URL` (or `controlplane_url` in the config file). A broker
with `FELIX_NODE_ID` and no URL fails earlier with:

```
FELIX_NODE_ID is set but FELIX_CONTROLPLANE_URL is not; there is nowhere to register
```

### No node credential

```
FELIX_NODE_ID is set but no node credential was provided; set FELIX_NODE_TOKEN or FELIX_NODE_TOKEN_FILE
```

A cluster member presents a credential on every call to the control plane.
See `FELIX_NODE_TOKEN` and `FELIX_NODE_REFRESH_TOKEN_FILE` in the
[environment reference](/felix/reference/environment-variables/#node-identity-and-membership).
A broker without `FELIX_NODE_ID` starts without one, but warns:

```
WARN FELIX_CONTROLPLANE_URL is set but no FELIX_NODE_TOKEN: the control plane will refuse the metadata sync, so no tenant, namespace, stream or cache will be learned from it
```

Clients of that broker then find no streams.

### No peer mTLS

```
this broker joins a cluster (FELIX_NODE_ID is set) without peer mTLS: anything that can reach FELIX_INTERNAL_BIND could act as a broker and rewrite replicas. Set FELIX_INTERNAL_TLS_CERT, FELIX_INTERNAL_TLS_KEY and FELIX_INTERNAL_TLS_CA, or set FELIX_INTERNAL_ALLOW_UNAUTHENTICATED=true if the internal port is reachable from brokers only
```

Give each broker a certificate whose DNS name is its `FELIX_NODE_ID`, signed by
a CA every broker trusts. Set `FELIX_INTERNAL_ALLOW_UNAUTHENTICATED=true` only
when a network policy keeps the internal port (default `0.0.0.0:5001`)
reachable from brokers alone.

### Port already in use

The broker binds three ports: client QUIC (`FELIX_QUIC_BIND`, UDP 5000), peer
QUIC (`FELIX_INTERNAL_BIND`, UDP 5001) and HTTP metrics and health
(`FELIX_BROKER_METRICS_BIND`, TCP 8080). The client and internal listeners must
not share a port, and that is refused at startup. Any other conflict is an
`Address already in use` error from the OS.

### A durable log refuses to open

A torn record at the end of the newest segment is repaired at startup. Damage
anywhere else is fatal, and the error names the shard, segment and position:
refusing to start beats silently losing acknowledged records. A segment
written by a newer build is refused the same way, which is why a storage format
upgrade does not roll back. See
[Durable Storage](/felix/architecture/durable-storage/) and
[Upgrades](/felix/deployment/upgrades/#storage-format-the-one-that-does-not-roll-back).

### The disk is full

A write or a log creation that finds no space fails with `storage full`, and
clients see `overloaded` with nothing written. `felix_storage_full_total`
counts them. Each new log reserves a whole segment up front
(`FELIX_DURABLE_SEGMENT_BYTES`, 256 MiB by default), and a stream shard with
consumer groups has three logs, so a disk can fill on the first group poll of
a new shard. Free space or lower the segment size; nothing needs repairing.

## Clients cannot connect

A started listener logs its address:

```
INFO quic listener started addr=0.0.0.0:5000
```

If it says `127.0.0.1`, remote clients cannot reach it; set `FELIX_QUIC_BIND`.
QUIC runs over UDP, so check that firewalls, security groups and load
balancers pass UDP on the client port. TCP tools such as `nc -z` tell you
nothing about it.

A handshake that times out on some networks and not others is often path MTU.
On Linux, keep `FELIX_MTU_UPPER_BOUND` at or below 6,553. Above it, the kernel
rejects UDP GSO batches and delivery stalls for good (see
[`FELIX_MTU_UPPER_BOUND`](/felix/reference/environment-variables/#felix_mtu_upper_bound)).

## Cluster problems

### The control plane refuses the heartbeat

```
WARN heartbeat failed; retrying with backoff error=heartbeat rejected (403 Forbidden): ... missing node.manage on node:broker-1 or cluster:*
```

The broker's credential lacks `node.manage` on its own node. Give it one that
grants `node.manage` on `node:<its FELIX_NODE_ID>` or `cluster:*`. A 401 means
the token is missing, malformed or expired. With a refresh token file the
broker renews its token before expiry, so check that the file is writable.

If registration itself is refused, the broker logs
`the control plane refused this identity; the broker is not a cluster member`
and exits. While heartbeats fail, `felix_broker_heartbeat_age_seconds` keeps
rising, and the broker stops serving its shards once its lease runs out.

### A peer is refused

```
WARN refusing an internal peer: its certificate does not match the identity it claims claimed=broker-2 detail=the peer's certificate is not issued to broker-2
```

The connecting broker's certificate does not carry its `FELIX_NODE_ID` as a DNS
name. Other details are `the peer presented no certificate` (that broker runs
without peer mTLS) and `the peer's certificate does not parse`.

### Replication has stopped for a replica

`felix_broker_replication_halted` counts the replicas a leader has stopped
shipping to. Which ones, and why, is on the leader's metrics port:

```bash
curl -s http://broker-1:8080/replication/halted
```

Each entry names the shard, the follower's node, the generation, how far the
follower got, a `reason` and a `remedy`:

| `reason` | Meaning |
| --- | --- |
| `diverged` | The follower holds different bytes at an offset the leader also holds. Its copy has to be rebuilt. |
| `needs_bootstrap` | The follower needs records retention has already removed from the leader, and holds records of its own. Its copy has to be rebuilt. |
| `fenced` | The follower knows a newer generation, so this broker no longer leads. It clears when the assignment feed catches up. If it persists, this broker is cut off from the control plane. |

The leader rebuilds `diverged` and `needs_bootstrap` followers itself, as many
at once as `FELIX_REPLICATION_REBUILD_MAX_CONCURRENT` allows (default `1`;
`0` leaves them all to an operator). The entry clears once the rebuild is done.

The control plane sees the same halts through the leader's replica reports:
`felix-controlplane admin replication` lists them in `HALTED`, and
`felix_shard_replicas_halted` counts them. Placement never moves a shard onto a
halted copy, and replaces a copy that stays halted for
`FELIX_SHARD_RESTORE_AFTER_MS`. A move to one by hand is refused with
`destination_halted`.

### `shard_unavailable`

The error's `detail.reason` says why:

| `reason` | Meaning |
| --- | --- |
| `moving` | The shard is moving and the move did not cut over within `FELIX_SHARD_MOVE_HOLD_MS` (default 2 s). Nothing was accepted. Retry after `detail.retry_after_ms`. |
| `not_assigned` | Placement has not assigned the shard. `felix-controlplane admin plan` says why it is waiting. |
| `owner_unavailable` | The leader is known but not live or not reachable. |
| `not_ready` | The leader has not finished opening the shard. |
| `stale` | This broker's routing view is behind the request's. It resolves as the assignment feed arrives. |
| `fenced` | The broker no longer leads the shard. |
| `region_not_routable` | The leader is in a region this broker has no bridge to (`FELIX_REGION_BRIDGES`). |

A durable shard whose only copy of the log is on a broker that is down stays
unassigned until that broker returns. `felix-controlplane admin abandon` gives
up its log and places it afresh, which loses data. See
[Moving shards by hand](/felix/deployment/moving-shards/#abandoning-a-shards-log).

## Publishes are refused or slow

### `publish queue full`

```
publish queue full; retry in 10 ms
```

The error is `overloaded` with `detail.reason = "publish_queue_full"`. Nothing
was queued, so it is safe to retry. `felix_tenant_publish_queue_full_total`
shows which tenants are being refused. Raise `FELIX_BROKER_PUB_QUEUE_DEPTH`
(default `64`) or `FELIX_PUBLISH_QUEUE_WAIT_MS` (default `2000`), or add
brokers.

### High latency

- Run release builds. Debug builds are 10 to 100 times slower.
- `FELIX_EVENT_BATCH_MAX_DELAY_US` is the longest an event waits for its batch
  to fill, so it is the latency floor batching adds.
- `FELIX_DISABLE_TIMINGS=1` turns off per-stage timing collection.
- A batched benchmark run (`batch > 1`) measures throughput, not request
  latency.

### Subscribers miss events

The default overflow policy is `drop_new`: a slow subscriber loses its own
events instead of slowing anyone else. Durable streams deliver log offsets, so
a jump in offsets is a drop. See [Publish/Subscribe](/felix/features/pubsub/).

## Memory

Flow-control windows times connections bound in-flight data. To use less:

```bash
export FELIX_CACHE_CONN_RECV_WINDOW="134217728"        # 128 MiB
export FELIX_CACHE_STREAM_RECV_WINDOW="33554432"       # 32 MiB
export FELIX_EVENT_CONN_RECV_WINDOW="134217728"
export FELIX_BROKER_PUB_QUEUE_DEPTH="32"               # default 64
export FELIX_SUBSCRIBER_QUEUE_CAPACITY="128"           # default 512
export FELIX_BROKER_PUBLISH_INFLIGHT_BYTES="33554432"  # default 64 MiB
```

The broker's client listeners buffer at most `FELIX_BROKER_PUB_CONN_RECV_WINDOW`
of unread data per connection, 16 MiB by default.

## Containers

### Image and version

Images are `ghcr.io/getfelix/felix-broker:<version>` and
`ghcr.io/getfelix/felix-controlplane:<version>` (`ghcr.io/gabloe` for 0.6.0-preview and earlier). The binary has no `--version`
flag; the image tag is the version.

### Health check failing

The image's health check polls `http://127.0.0.1:8080/ready`. It fails while
the broker drains, which is intended, and it fails if
`FELIX_BROKER_METRICS_BIND` moves the metrics port. For orchestrator probes,
use `/ready` for readiness and `/live` for liveness.

### Permission denied on the data directory

The image runs as uid and gid 65532, and `/var/lib/felix` is its volume. Point
`FELIX_DURABLE_STORAGE_DIR` there and make the mounted volume writable by that
uid:

```bash
chown -R 65532:65532 /path/to/volume
```

In Kubernetes, `securityContext.fsGroup: 65532` does the same.

### CrashLoopBackOff

The reason is in the previous container's log, and it is usually one of the
startup refusals above:

```bash
kubectl logs felix-broker-0 -n felix --previous
```

## Debugging

### Logging

```bash
export RUST_LOG="felix_broker_service=debug,felix_replication=debug"
```

The crates are `felix_broker_service`, `felix_broker`, `felix_replication`,
`felix_storage`, `felix_wire` and `felix_transport`.

### Backtraces and symbols

```bash
export RUST_BACKTRACE=1
CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release -p felix-broker-service --bin felix-broker
./target/release/felix-broker
```

### Tracing and hot-path timings

OTLP trace export turns on when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. The
hot-path timing histograms need the `telemetry` feature, which is off by
default: `cargo build --release -p felix-broker-service --features telemetry`.

### Latency on your machine

The latency demo runs its own in-process broker:

```bash
cargo run --release -p felix-broker-service --features demo --bin latency-demo -- \
  --binary --fanout 1 --batch 1 --payload 1024 --total 5000
```

## Reporting an issue

Open an issue at [github.com/GetFelix/felix/issues](https://github.com/GetFelix/felix/issues)
with the image tag or commit, the output of `felix-broker --print-config` (the
credential is redacted), the full error and the steps to reproduce it.
