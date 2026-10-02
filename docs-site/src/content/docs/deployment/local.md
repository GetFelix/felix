---
title: "Local Development Deployment"
description: "Run a Felix cluster on your own machine, run a broker by hand, tune it, and run the demos."
---

Running Felix directly on your machine: a local cluster in one command, a
broker by hand when you need one, the settings development needs, and the
demos.

## Prerequisites

- **Rust 1.97.1 or later**, from [rustup](https://rustup.rs/)
- **Git**
- Optional: [Task](https://taskfile.dev/) for the shortcuts CI uses

## Build

```bash
git clone https://github.com/gabloe/felix.git
cd felix
cargo build --workspace --release
```

:::tip[Release vs debug builds]
Use release builds for anything you measure. A debug build is several times slower.
:::

## A local cluster

A broker does not run alone. It authenticates every client against keys it
fetches from a control plane, and it reads its streams from there. The
`felix-cluster` tool starts a control plane, mints the credentials, and starts
the brokers:

```bash
task cluster:up
# or, without Task:
cargo build --release -p felix-broker-service --bin felix-broker
cargo run --release -p felix-cluster -- up --nodes 3
```

It prints the control-plane URL, each broker's client and metrics address, and
who owns each shard, then holds the cluster until you press Ctrl-C. Each
process gets free ports, so nothing collides with what you already run. It
also writes a session file, `felix-cluster.json` in your system temp
directory, with the tenant, namespace, client token and broker addresses.
`subscribe`, `publish` and `owners` read that file to attach to the running
cluster. The [Quickstart](/felix/getting-started/quickstart/) walks through
them.

The tool runs the broker binary from `target/`, which is why the broker is
built first. Set `FELIX_CLUSTER_VERBOSE=1` to see the brokers' own logs.

## Running a broker by hand

Start a control plane. It keeps its metadata in memory unless you give it
`FELIX_CONTROLPLANE_POSTGRES_URL`. Both processes serve metrics on port 8080 by
default, so move one:

```bash
FELIX_CONTROLPLANE_METRICS_BIND=127.0.0.1:9091 \
  cargo run --release -p felix-controlplane-service
```

Then the broker, pointed at it:

```bash
FELIX_CONTROLPLANE_URL=http://127.0.0.1:8443 \
FELIX_NODE_TOKEN_FILE=./felix-node-token \
  cargo run --release -p felix-broker-service
```

`FELIX_CONTROLPLANE_URL` is required. Without it the broker logs
`broker started` and then exits with
`FELIX_CONTROLPLANE_URL must be set for auth`.

The node token is what the broker reads the control plane's metadata feeds
with. Without it the broker still starts, warns that
`the control plane will refuse the metadata sync, so no tenant, namespace, stream or cache will be learned from it`,
and serves no streams. The token comes out of the control plane's day-0
bootstrap and a token exchange with your identity provider; the
[Docker Compose page](/felix/deployment/docker-compose/#broker-credential)
shows the requests. Felix has no built-in development login, which is why
`felix-cluster` mints the tokens itself.

A started broker logs a `quic listener started` line with its address for
each client listener. By default that is UDP `0.0.0.0:5000`, with metrics and
health on TCP `0.0.0.0:8080`. The broker generates a self-signed certificate
for `localhost` at each start unless `FELIX_TLS_CERT` and `FELIX_TLS_KEY` are
set. `FELIX_TLS_CERT_EXPORT` writes the generated one to a file a client can
trust.

### Checking a broker

```bash
curl http://localhost:8080/ready     # 200 when serving, 503 while starting or draining
curl http://localhost:8080/live      # 200 while the process is up
curl http://localhost:8080/metrics   # Prometheus text format
```

A durable broker (`FELIX_DURABLE_STORAGE_DIR` set) answers 503 on `/ready`
until it has synced its catalog from the control plane.

## Configuration

The broker reads its built-in defaults, then `FELIX_*` environment variables,
then the YAML file at `FELIX_BROKER_CONFIG`, each overriding the one before.

### Environment variables

```bash
export FELIX_QUIC_BIND="0.0.0.0:5001"
export FELIX_BROKER_METRICS_BIND="0.0.0.0:8081"
export FELIX_EVENT_BATCH_MAX_DELAY_US="100"
```

The broker warns at startup about any `FELIX_*` variable it does not read,
which catches typos. The metrics variable is `FELIX_BROKER_METRICS_BIND`
because the control plane has its own.

### Config file

```yaml
# /tmp/felix-dev.yml
quic_bind: "0.0.0.0:5000"
metrics_bind: "0.0.0.0:8080"
controlplane_url: "http://127.0.0.1:8443"
controlplane_sync_interval_ms: 2000

ack_on_commit: false
max_frame_bytes: 16777216          # 16 MiB

publish_queue_wait_timeout_ms: 2000
ack_wait_timeout_ms: 2000
control_stream_drain_timeout_ms: 50

pub_conn_recv_window: 16777216     # 16 MiB
pub_stream_recv_window: 16777216   # 16 MiB
cache_send_window: 268435456       # 256 MiB

event_batch_max_events: 64
event_batch_max_bytes: 65536       # 64 KiB
event_batch_max_delay_us: 250

fanout_batch_size: 64
pub_workers_per_conn: 4
pub_queue_depth: 64
subscriber_queue_capacity: 512
subscriber_writer_lanes: 4
subscriber_lane_queue_depth: 64
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto

disable_timings: false
```

```bash
FELIX_BROKER_CONFIG=/tmp/felix-dev.yml cargo run --release -p felix-broker-service
```

An unknown key fails startup, and so does a `FELIX_BROKER_CONFIG` path that
does not exist. Without the variable the broker reads
`/usr/local/felix/config.yml` if it exists and carries on without it if not.
The [configuration reference](/felix/reference/configuration/) lists every
key.

### Latency and throughput settings

For the lowest latency with one subscriber, flush every event at once:

```bash
export FELIX_EVENT_BATCH_MAX_DELAY_US="50"
export FELIX_EVENT_BATCH_MAX_EVENTS="1"
export FELIX_FANOUT_BATCH="1"
export FELIX_DISABLE_TIMINGS="1"
```

For throughput, batch more:

```bash
export FELIX_EVENT_BATCH_MAX_DELAY_US="1000"
export FELIX_EVENT_BATCH_MAX_EVENTS="256"
export FELIX_EVENT_BATCH_MAX_BYTES="1048576"  # 1 MiB
export FELIX_FANOUT_BATCH="128"
```

### Client settings

Connection pools and receive windows on the client side are read by the Rust
client's `ClientConfig::from_env_or_yaml`, not by the broker:

```bash
export FELIX_EVENT_CONN_POOL="8"
export FELIX_EVENT_CONN_RECV_WINDOW="268435456"   # 256 MiB
export FELIX_EVENT_STREAM_RECV_WINDOW="67108864"  # 64 MiB
export FELIX_CACHE_CONN_POOL="8"
export FELIX_CACHE_STREAMS_PER_CONN="4"
```

Larger windows absorb bursts without flow-control stalls, but memory grows
with window size times pool size. Lower them if a client's memory use is too
high.

## Demos

The demo binaries start their own broker in-process on a random local port.
They do not connect to a broker you started, and they need no control plane.

```bash
# Publish, subscribe, fan out
cargo run --release -p felix-broker-service --features demo --bin pubsub-demo-simple

# Cache put/get latency across payload sizes
cargo run --release -p felix-broker-service --features demo --bin cache-demo

# A durable stream survives a crash; an in-memory one does not
cargo run --release -p felix-broker-service --bin durable-restart-demo

# Consumer groups: distribution, redelivery, dead letters
cargo run --release -p felix-broker-service --bin queue-semantics-demo

# Multi-tenant alerts. Flags: --alerts=10 --last-n=5 --drop-subscriber
cargo run --release -p felix-broker-service --bin pubsub-demo-notifications

# Orders and payments. Flags: --orders=12 --duplicate-every=5 --kill-worker=payments
cargo run --release -p felix-broker-service --bin pubsub-demo-orders
```

The latency demo takes flags for the shape of the run:

```bash
cargo run --release -p felix-broker-service --features demo --bin latency-demo -- \
    --binary --fanout 10 --batch 64 --payload 4096 --total 10000 --warmup 500
```

`--binary` uses the binary publish encoding, `--fanout` sets the number of
subscribers, `--batch` the messages per publish, `--payload` the size in
bytes, and `--total` and `--warmup` the measured and discarded message counts.
`--help` lists the rest.

Two demos are separate crates outside the workspace and run a real control
plane with token exchange:

```bash
# Live RBAC changes, against an in-memory control plane
cargo run --manifest-path demos/rbac-live/Cargo.toml

# Tokens for one tenant cannot reach another's data. Needs Postgres (task pg:up)
cargo run --manifest-path demos/cross_tenant_isolation/Cargo.toml
```

## Task commands

```bash
task build
task test
task lint
task fmt

task cluster:up                  # local cluster, held until Ctrl-C
task cluster:demo                # the cross-broker story in one pane

task demo:pubsub
task demo:cache
task demo:latency
task demo:queues
task demo:notifications
task demo:orders
task demo:slow-consumer
task demo:state-divergence
task demo:rbac-live
task demo:cross-tenant-isolation

task conformance
task coverage
```

`Taskfile.yml` has the full list.

## Logging

Logs go to stdout. `RUST_LOG` sets the level, `info` by default:

```bash
RUST_LOG=debug cargo run --release -p felix-broker-service
RUST_LOG="felix_broker=debug,felix_wire=trace" cargo run --release -p felix-broker-service
```

## Troubleshooting

### Address already in use

Move the ports:

```bash
export FELIX_QUIC_BIND="0.0.0.0:5001"
export FELIX_BROKER_METRICS_BIND="0.0.0.0:8081"
```

If the control plane runs on the same machine, it also wants 8080 for metrics.
Set `FELIX_CONTROLPLANE_METRICS_BIND` on one of them.

### The broker exits right after `broker started`

`FELIX_CONTROLPLANE_URL` is not set. Read the error line that follows.

### A client cannot connect

Check that the broker is listening and that the client trusts its certificate:

```bash
lsof -i UDP:5000
RUST_LOG=debug cargo run --release -p felix-broker-service
```

A generated certificate changes on every start, so a client that trusted the
previous one fails the handshake after a restart.

### Build failures

```bash
rustc --version  # 1.97.1 or later
rustup update
```

## Next steps

- **Learn the client API**: [Client SDK Guide](/felix/clients/rust/)
- **Deploy with Docker**: [Docker Compose Setup](/felix/deployment/docker-compose/)
- **Production deployment**: [Kubernetes Guide](/felix/deployment/kubernetes/)
- **Tune performance**: [Performance Guide](/felix/features/performance/)
- **Configure fully**: [Configuration Reference](/felix/reference/configuration/)
