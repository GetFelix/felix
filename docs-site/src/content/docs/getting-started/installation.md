---
title: "Installation"
description: "Build Felix from source, check the build, and find the released binaries and container images."
---

Each release attaches Linux x86_64 builds of `felix-broker` and
`felix-controlplane` to its GitHub release and publishes both as container
images (see [Docker](#docker-alternative)). Everything else, including the
demos, the local cluster tool and the clients, builds from source. That takes
a Rust toolchain and a few minutes.

To install only the command-line tool, use Homebrew
(`brew install getfelix/tap/felixctl`), cargo or a release archive; see
[felixctl](/getting-started/felixctl/#install). The rest of this page builds
Felix from source.

## System Requirements

- **Operating System:** Linux, macOS, or Windows (WSL2 recommended)
- **Rust:** 1.97.1 or later
- **Memory:** 4 GB minimum, 8 GB recommended for development
- **Disk:** 2 GB for build artifacts
- **Network:** For QUIC, ensure UDP traffic is allowed on your firewall

## Install Rust

Felix requires Rust 1.97.1 or later. Install using [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Follow the prompts to complete installation. Then verify:

```bash
rustc --version
cargo --version
```

Expected output:

```
rustc 1.97.1 (or later)
cargo 1.97.1 (or later)
```

## Clone the Repository

```bash
git clone https://github.com/GetFelix/felix.git
cd felix
```

## Build from Source

### Development Build

For development and debugging with full error information:

```bash
cargo build --workspace
```

Binaries will be in `target/debug/`.

### Release Build

For performance testing and production use:

```bash
cargo build --workspace --release
```

Binaries will be in `target/release/`.

:::caution[Performance Difference]
Release builds are **significantly faster** than debug builds. Always use `--release` for any performance testing or benchmarking.
:::
### Build Specific Crates

Build only the broker service:

```bash
cargo build -p felix-broker-service --release
```

Build only the client library:

```bash
cargo build -p felix-client --release
```

## Verify Installation

### Run Tests

Verify everything is working:

```bash
task test
```

That is what CI runs. Without Task, build the load generator first, because
one cluster test runs its prebuilt binary:

```bash
cargo build -p felix-loadgen
cargo test --workspace
```

The control plane's Postgres tests sit behind a feature and do not run in a
plain `cargo test`. `task test` starts a Postgres container, when Docker or
Podman is available, and runs them too. See [Docker or Podman](/getting-started/containers/).

### Run the Conformance Suite

Felix includes a wire protocol conformance runner to validate correct framing and message encoding:

```bash
cargo run -p felix-conformance
```

It starts a broker in-process, runs the checks against it, and ends with:

```
== Felix Conformance Runner ==
...
Conformance checks passed.
```

### Start a cluster

A broker authenticates every connection against a control plane, so it does not
run alone. `felix-cluster` starts a control plane, mints the credentials and
brings up the brokers:

```bash
cargo run --release -p felix-cluster -- up --nodes 3
```

```
starting a 3-node cluster...

cluster up.

control plane   http://127.0.0.1:52704

node       client                 metrics
broker-0   127.0.0.1:53348        127.0.0.1:52706
broker-1   127.0.0.1:65027        127.0.0.1:52707
broker-2   127.0.0.1:50410        127.0.0.1:52708

placeable: broker-0, broker-1, broker-2
shard ownership:
  stream/t1/ns/orders/0 -> broker-2

session   /tmp/felix-cluster.json

holding the cluster. press Ctrl-C to tear it down.
```

Every address is chosen free at startup, so nothing collides with what you are
already running. The [Quickstart](/getting-started/quickstart/) goes on to
publish and subscribe against it; running brokers yourself is covered there too.

### Run a Demo

Verify end-to-end functionality with a self-contained demo (no separate broker required):

```bash
cargo run --release -p felix-broker-service --features demo --bin pubsub-demo-simple
```

Other demos you can try (including a control-plane RBAC mutation demo):

```bash
cargo run --release -p felix-broker-service --features demo --bin cache-demo
cargo run --release -p felix-broker-service --features demo --bin latency-demo
cargo run --release -p felix-broker-service --bin durable-restart-demo
cargo run --release -p felix-broker-service --bin queue-semantics-demo
cargo run --release -p felix-broker-service --bin pubsub-demo-notifications
cargo run --release -p felix-broker-service --bin pubsub-demo-orders
cargo run --manifest-path demos/rbac-live/Cargo.toml
cargo run --manifest-path demos/cross_tenant_isolation/Cargo.toml
```

The cross-tenant isolation demo needs Postgres (`task pg:up`).

See the [Demos Overview](/demos/overview/) for details and expected output.

## Optional Tools

### Task Runner

Install [Task](https://taskfile.dev/) for convenient commands:

**macOS/Linux:**

```bash
sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin
```

**Using Homebrew:**

```bash
brew install go-task/tap/go-task
```

Then you can use:

```bash
task build      # Build everything
task test       # Run tests
task fmt        # Format code
task lint       # Run linters
```

### Cargo Tools

Install additional cargo extensions for development:

```bash
# Code coverage
cargo install cargo-llvm-cov

# Security auditing
cargo install cargo-deny --version 0.19.0 --locked

```

## Build Customization

### Feature Flags

Felix supports optional feature flags:

#### Telemetry

Enable detailed per-stage timing instrumentation:

```bash
cargo build --release -p felix-broker-service --features telemetry
```

:::note[Performance impact]
Telemetry adds instrumentation to the publish and delivery paths, and high fanout or batching can make its cost show in tail latency. The release binaries and images are built without it.
:::
### All Demos

Build every demo binary in the broker service, including the ones behind the
`demo` feature:

```bash
cargo build --release -p felix-broker-service --features demo --bins
```

## Platform-Specific Notes

### Linux

For high throughput, raise the UDP buffer limits:

```bash
sudo sysctl -w net.core.rmem_max=26214400
sudo sysctl -w net.core.wmem_max=26214400
```

### macOS

Works well on macOS 11 (Big Sur) and later. No special configuration needed.

### Windows (WSL2)

Use WSL2 for best compatibility:

1. Install WSL2: [Microsoft Guide](https://learn.microsoft.com/en-us/windows/wsl/install)
2. Install Ubuntu or Debian
3. Follow Linux instructions inside WSL2

Native Windows support is not currently tested.

## Docker (Alternative)

Released images are on GHCR and pull without credentials. The commands work
with Podman as written once `docker` is replaced with `podman`, except where
[Docker or Podman](/getting-started/containers/) says otherwise. Releases before 0.6.0-preview.2 are under `ghcr.io/gabloe`, the project's previous owner; later releases publish under `ghcr.io/getfelix`.

```bash
docker run -p 5000-5003:5000-5003/udp -p 8080:8080 \
  -e FELIX_CONTROLPLANE_URL=http://<control-plane-host>:8443 \
  -e FELIX_NODE_TOKEN_FILE=/etc/felix/node.token \
  -v /path/to/node.token:/etc/felix/node.token:ro \
  ghcr.io/getfelix/felix-broker:0.6.0-preview.2
```

The broker binds up to four client ports from `5000`, depending on its cores
(see [`FELIX_QUIC_LISTENERS`](/reference/environment-variables/#felix_quic_listeners)),
and tells clients to use all of them, so publish the whole range.

A broker authenticates every client against its tenant's signing keys, which it
fetches from the control plane, and it reads its streams from there with a
node credential. Without `FELIX_CONTROLPLANE_URL` it logs `broker started` and
exits on the next line. Without the credential it runs but serves no streams.
[Docker Compose](/deployment/docker-compose/) wires the pair together; for
a local cluster with nothing to configure, `felix-cluster up` is quicker (see
the [Quickstart](/getting-started/quickstart/)).

Each release publishes the full version (`0.6.0-preview.2`). A release without a
pre-release suffix also publishes the minor series (`0.6`) and `latest`. Use a
full version tag in anything you keep; the other two move. Images are
signed by digest. See [Kubernetes](/deployment/kubernetes/) for the
`cosign verify` invocation.

To build one instead, for a change you have not released:

```bash
# Build the broker image
docker build -t felix-broker -f docker/broker.Dockerfile .

# Run what you built, with the same variables and mount as above
docker run -p 5000-5003:5000-5003/udp -p 8080:8080 -e FELIX_CONTROLPLANE_URL=... felix-broker
```

### Nightly builds

Every night that `main` has changed and its CI passed, the same images are
published as `nightly` and `nightly-YYYYMMDD`:

```bash
docker pull ghcr.io/getfelix/felix-broker:nightly
docker pull ghcr.io/getfelix/felix-controlplane:nightly
docker pull ghcr.io/getfelix/felixctl:nightly
```

The binaries, Python wheels and Node addons are on the
[`nightly` pre-release](https://github.com/GetFelix/felix/releases/tag/nightly),
which is replaced each time. A nightly has passed CI and one publish and
subscribe through its images, nothing more. Use it to try what is coming, not
in production. Dated tags are kept for about two weeks.

### Control Plane Container

The same, for the control plane:

```bash
# Or build it: docker build -t felix-controlplane -f docker/controlplane.Dockerfile .

# Without a Postgres URL it keeps its metadata in memory. Under Podman the
# host is host.containers.internal.
docker run -p 8443:8443 \
  -e FELIX_CONTROLPLANE_POSTGRES_URL=postgres://postgres:postgres@host.docker.internal:55432/postgres \
  ghcr.io/getfelix/felix-controlplane:0.6.0-preview.2
```

See [Docker Compose Guide](/deployment/docker-compose/) for orchestrated deployments.

## Troubleshooting

### Linker Errors

Use `lld` for faster linking (optional):

```bash
# Install lld
sudo apt-get install lld  # Debian/Ubuntu
brew install llvm         # macOS

# Configure Rust to use it
mkdir -p .cargo
cat > .cargo/config.toml << EOF
[target.x86_64-unknown-linux-gnu]
linker = "clang"
rustflags = ["-C", "link-arg=-fuse-ld=lld"]
EOF
```

### Out of Memory

If the build runs out of memory:

```bash
# Reduce parallel jobs
cargo build --release -j 2
```

## Next Steps

- [Quickstart Guide](/getting-started/quickstart/) - Run your first Felix deployment
- [Building & Testing](/development/building/) - Development workflow
- [Configuration](/reference/configuration/) - Customize Felix behavior
