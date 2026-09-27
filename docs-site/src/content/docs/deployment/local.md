---
title: "Local Development Deployment"
---

Running Felix directly on your machine: build it, start a broker, point clients at it, and switch between the configurations development actually needs.

## Prerequisites

Before running Felix locally, ensure you have:

- **Rust 1.97.1 or later**: Install via [rustup](https://rustup.rs/)
- **Git**: For cloning the repository
- **Optional**: [Task](https://taskfile.dev/) for convenience commands
- **At least 4GB RAM**: Recommended for comfortable development
- **Ports available**: Default ports 5000 (QUIC) and 8080 (metrics)

## Quick Start

### Clone and Build

```bash
# Clone the repository
git clone https://github.com/gabloe/felix.git
cd felix

# Build the workspace in release mode
cargo build --workspace --release
```

:::tip[Release vs Debug Builds]
Always use release builds (`--release`) for performance testing. Debug builds have significantly higher overhead and can show 10-100x worse latency characteristics. Use debug builds only for debugging with tools like `gdb` or `lldb`.
:::
### Start the Broker

Run the broker with default settings:

```bash
cargo run --release -p felix-broker-service
```

**Expected output:**

```
2026-01-25T10:00:00.000Z INFO felix_broker: Starting Felix broker
2026-01-25T10:00:00.001Z INFO felix_broker: QUIC listening on 0.0.0.0:5000
2026-01-25T10:00:00.001Z INFO felix_broker: Metrics server on 0.0.0.0:8080
```

The broker is now accepting connections on:

- **QUIC data plane**: `0.0.0.0:5000` (UDP)
- **Metrics/health HTTP**: `0.0.0.0:8080` (TCP)

### Verify the Broker

Check that the broker is running:

```bash
# Check QUIC listener (requires lsof or ss)
lsof -i UDP:5000

# Check metrics endpoint
curl http://localhost:8080/healthz
```

## Configuration Methods

Felix supports three configuration methods, applied in this order (later sources override earlier ones):

1. **Built-in defaults**: Sensible defaults for local development
2. **Environment variables**: Quick overrides via `FELIX_*` variables
3. **YAML config file**: Structured configuration for complex setups

### Using Environment Variables

The simplest way to configure Felix locally:

```bash
# Change broker ports
export FELIX_QUIC_BIND="0.0.0.0:5001"
export FELIX_BROKER_METRICS_BIND="0.0.0.0:8081"

# Enable publish acknowledgements
export FELIX_ACK_ON_COMMIT="true"

# Tune batching for lower latency
export FELIX_EVENT_BATCH_MAX_DELAY_US="100"

# Run broker with custom config
cargo run --release -p felix-broker-service
```

### Using a Config File

For more complex configurations, create a YAML file:

**`/tmp/felix-dev.yml`:**

```yaml
# Network bindings
quic_bind: "0.0.0.0:5000"
metrics_bind: "0.0.0.0:8080"

# Optional control plane
controlplane_url: "http://localhost:8443"
controlplane_sync_interval_ms: 2000

# Publishing behavior
ack_on_commit: false
max_frame_bytes: 16777216  # 16 MiB

# Timeouts
publish_queue_wait_timeout_ms: 2000
ack_wait_timeout_ms: 2000
control_stream_drain_timeout_ms: 50

# Cache flow control
cache_conn_recv_window: 268435456  # 256 MiB
cache_stream_recv_window: 67108864 # 64 MiB
cache_send_window: 268435456       # 256 MiB

# Event batching
event_batch_max_events: 64
event_batch_max_bytes: 65536       # 64 KiB
event_batch_max_delay_us: 250

# Fanout and workers
fanout_batch_size: 64
pub_workers_per_conn: 4
pub_queue_depth: 64
subscriber_queue_capacity: 512
subscriber_writer_lanes: 4
subscriber_lane_queue_depth: 64
max_subscriber_writer_lanes: 8
subscriber_lane_shard: auto

# Performance
disable_timings: false
```

**Run with custom config:**

```bash
FELIX_BROKER_CONFIG=/tmp/felix-dev.yml cargo run --release -p felix-broker-service
```

:::caution[Config File Priority]
If `FELIX_BROKER_CONFIG` is set and the file doesn't exist, the broker will fail to start. Without this variable, the broker looks for `/usr/local/felix/config.yml` but continues with defaults if not found.
:::
## Common Development Scenarios

### Scenario 1: Low-Latency Testing

Optimize for minimum latency (single subscriber, small batches):

```bash
export FELIX_EVENT_BATCH_MAX_DELAY_US="50"
export FELIX_EVENT_BATCH_MAX_EVENTS="1"
export FELIX_FANOUT_BATCH="1"
export FELIX_DISABLE_TIMINGS="1"

cargo run --release -p felix-broker-service
```

### Scenario 2: High-Throughput Testing

Optimize for maximum throughput (large batches, higher fanout):

```bash
export FELIX_EVENT_BATCH_MAX_DELAY_US="1000"
export FELIX_EVENT_BATCH_MAX_EVENTS="256"
export FELIX_EVENT_BATCH_MAX_BYTES="1048576"  # 1 MiB
export FELIX_FANOUT_BATCH="128"

cargo run --release -p felix-broker-service
```

### Scenario 3: Multi-Client Development

Run multiple clients connecting to the same broker:

```bash
# Terminal 1: Start broker
cargo run --release -p felix-broker-service

# Terminal 2: Run subscriber demo
cargo run --release -p felix-broker-service --features demo --bin pubsub-demo-simple

# Terminal 3: Run another client
cargo run --release -p felix-broker-service --features demo --bin cache-demo
```

### Scenario 4: Testing Control Plane Integration

Set up broker to connect to a local control plane:

```bash
export FELIX_CONTROLPLANE_URL="http://localhost:8443"
export FELIX_CONTROLPLANE_SYNC_INTERVAL_MS="1000"
# A Felix token carrying node.view:cluster:*; the metadata feeds refuse
# anything else. See "Broker credential" on the Docker Compose page for how
# bootstrap and token exchange produce one.
export FELIX_NODE_TOKEN="<felix token>"

cargo run --release -p felix-broker-service
```

## Performance Profiles

Felix includes pre-tuned performance profiles for different use cases:

### Balanced Profile (Default)

Good starting point for mixed workloads:

```bash
export FELIX_EVENT_CONN_POOL="8"
export FELIX_EVENT_CONN_RECV_WINDOW="268435456"
export FELIX_EVENT_STREAM_RECV_WINDOW="67108864"
export FELIX_EVENT_SEND_WINDOW="268435456"
export FELIX_EVENT_BATCH_MAX_DELAY_US="250"
export FELIX_CACHE_CONN_POOL="8"
export FELIX_CACHE_STREAMS_PER_CONN="4"

cargo run --release -p felix-broker-service
```

### High-Memory Profile

For burst tolerance with more memory:

```bash
export FELIX_EVENT_CONN_POOL="8"
export FELIX_EVENT_CONN_RECV_WINDOW="536870912"   # 512 MiB
export FELIX_EVENT_STREAM_RECV_WINDOW="134217728"  # 128 MiB
export FELIX_EVENT_SEND_WINDOW="536870912"         # 512 MiB
export FELIX_EVENT_BATCH_MAX_DELAY_US="250"
export FELIX_CACHE_CONN_POOL="8"
export FELIX_CACHE_STREAMS_PER_CONN="4"
export FELIX_DISABLE_TIMINGS="1"

cargo run --release -p felix-broker-service
```

:::note[Memory vs Performance]
Larger window sizes allow absorbing traffic bursts without flow-control stalls, but multiply memory usage with connection pools. Monitor actual RSS to understand memory pressure.
:::
## Running Demos

Felix includes several demonstration programs:

:::note[Self-contained demos]
The demo binaries start an in-process broker and QUIC server on a random local port.
You do not need to run the broker separately for these demos.
:::
### Pub/Sub Demo

```bash
cargo run --release -p felix-broker-service --features demo --bin pubsub-demo-simple
```

Demonstrates:
- Subscribing to a stream
- Publishing messages
- Receiving events with fanout

### Cache Demo

```bash
cargo run --release -p felix-broker-service --features demo --bin cache-demo
```

Benchmarks cache operations:
- `put` with TTL
- `get_hit` (key exists)
- `get_miss` (key doesn't exist)

### Latency Demo

```bash
# Basic run
cargo run --release -p felix-broker-service --features demo --bin latency-demo

# Custom configuration
cargo run --release -p felix-broker-service --features demo --bin latency-demo -- \
    --binary \
    --fanout 10 \
    --batch 64 \
    --payload 4096 \
    --total 10000 \
    --warmup 500
```

**Parameters:**

- `--binary`: Use binary batch encoding (higher throughput)
- `--fanout N`: Number of concurrent subscribers
- `--batch N`: Messages per batch
- `--payload N`: Payload size in bytes
- `--total N`: Total messages to publish
- `--warmup N`: Warmup messages before measurement

### Scenario Demos

#### Notifications (Multi-tenant alerts)

```bash
cargo run --release -p felix-broker-service --bin pubsub-demo-notifications
```

Optional flags: `--alerts=10`, `--last-n=5`, `--drop-subscriber`.

#### Orders/Payments Pipeline

```bash
cargo run --release -p felix-broker-service --bin pubsub-demo-orders
```

Optional flags: `--orders=12`, `--duplicate-every=5`, `--kill-worker=payments`.

#### Live RBAC Policy Change

```bash
cargo run --manifest-path demos/rbac-live/Cargo.toml
```

Demonstrates live RBAC updates via the control plane with real token exchange
and broker authorization.
Uses an in-memory control-plane store (no Postgres required).

#### Cross-Tenant Isolation

```bash
cargo run --manifest-path demos/cross_tenant_isolation/Cargo.toml
```

Demonstrates that tokens minted for one tenant cannot access another tenant's
resources, even when the same upstream identity is used.
Uses a Postgres-backed control plane (requires `task pg:up` or an external DB).

## Using Task Commands

If you have [Task](https://taskfile.dev/) installed:

```bash
# Build workspace
task build

# Run tests
task test

# Run linter
task lint

# Format code
task fmt

# Run demos
task demo:pubsub
task demo:cache
task demo:latency
task demo:notifications
task demo:orders
task demo:rbac-live
task demo:cross-tenant-isolation

# Run conformance tests
task conformance

# Generate coverage report
task coverage
```

See `Taskfile.yml` for all available tasks.

## Monitoring Local Development

### Metrics Endpoint

The broker exposes metrics on the HTTP port:

```bash
# Health check
curl http://localhost:8080/healthz

# Prometheus metrics (if enabled)
curl http://localhost:8080/metrics
```

### Structured Logging

Felix logs in structured format to stdout:

```
2026-01-25T10:00:01.123Z INFO felix_broker: Connection accepted remote_addr=127.0.0.1:54321
2026-01-25T10:00:01.456Z INFO felix_broker: Subscription created tenant=dev namespace=test stream=events
2026-01-25T10:00:02.789Z WARN felix_broker: Publish queue pressure queue_depth=512
```

Control log verbosity with `RUST_LOG`:

```bash
# Debug level (verbose)
export RUST_LOG="debug"

# Info level (default)
export RUST_LOG="info"

# Specific module
export RUST_LOG="felix_broker=debug,felix_wire=trace"

cargo run --release -p felix-broker-service
```

## Troubleshooting

### Port Already in Use

**Error:** `Address already in use (os error 48)`

**Solution:** Change the ports:

```bash
export FELIX_QUIC_BIND="0.0.0.0:5001"
export FELIX_BROKER_METRICS_BIND="0.0.0.0:8081"
```

### Build Failures

**Error:** Compilation errors or missing dependencies

**Solution:** Ensure correct Rust version:

```bash
rustc --version  # Should be 1.97.1 or later
rustup update
cargo clean
cargo build --release
```

### Connection Refused

**Error:** Client cannot connect to broker

**Solution:** Verify broker is running and listening:

```bash
# Check processes
ps aux | grep broker

# Check UDP listener
lsof -i UDP:5000

# Check logs
RUST_LOG=debug cargo run --release -p felix-broker-service
```

### High Memory Usage

**Issue:** Broker consuming excessive memory

**Solution:** Reduce window sizes:

```bash
export FELIX_EVENT_CONN_RECV_WINDOW="134217728"   # 128 MiB
export FELIX_EVENT_STREAM_RECV_WINDOW="33554432"  # 32 MiB
export FELIX_CACHE_CONN_RECV_WINDOW="134217728"
```

### Slow Performance

**Issue:** Unexpectedly high latency

**Solution:**

1. **Use release builds**: Debug builds are 10-100x slower
2. **Disable timings**: `export FELIX_DISABLE_TIMINGS="1"`
3. **Check batching**: Increase batch sizes for throughput
4. **Profile with `perf`**: Identify hot paths

```bash
cargo build --release
FELIX_DISABLE_TIMINGS=1 cargo run --release -p felix-broker-service
```

## Next Steps

- **Learn the client API**: [Client SDK Guide](/felix/clients/rust/)
- **Deploy with Docker**: [Docker Compose Setup](/felix/deployment/docker-compose/)
- **Production deployment**: [Kubernetes Guide](/felix/deployment/kubernetes/)
- **Tune performance**: [Performance Guide](/felix/features/performance/)
- **Configure fully**: [Configuration Reference](/felix/reference/configuration/)
