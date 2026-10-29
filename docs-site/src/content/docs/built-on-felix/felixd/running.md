---
title: "Running felixd"
description: "The felixd subcommands and flags, the environment variables that point it at Felix, what it needs from a Felix cluster, and a quickstart that starts a broker, submits a workload and checks it."
---

felixd is one binary with four subcommands. There are no release binaries yet
([#23](https://github.com/GetFelix/felixd/issues/23)), so build it from source with
Rust 1.97 or later:

```bash
git clone https://github.com/GetFelix/felixd
cd felixd
cargo build --release -p felixd      # target/release/felixd
```

## Subcommands

| Command | What it does |
|---|---|
| `felixd run` | Submits a workload: stores its module, creates its streams and writes its spec |
| `felixd agent` | Runs on every node. Claims workloads and runs them while it holds their leases |
| `felixd check` | Replays each workload over its input and reports what is wrong with its output stream |
| `felixd replay` | Rebuilds a workload's state at an input offset or time, or compares a replay with the recorded outputs |

`felixd ps` and `felixd drain` are in the design and do not exist
([#17](https://github.com/GetFelix/felixd/issues/17)). `felixd --version` prints the
version.

### Connection

Every subcommand takes the same connection settings, as flags or environment
variables. The variables are the ones `felixctl` reads, plus `FELIX_NAMESPACE`.

| Flag | Variable | Default | Meaning |
|---|---|---|---|
| `--brokers` | `FELIX_BROKERS` | required | Broker addresses as `host:port`, comma separated. One is enough, the rest are discovered |
| `--server-name` | `FELIX_SERVER_NAME` | `localhost` | The name on the brokers' certificate |
| `--tenant` | `FELIX_AUTH_TENANT` | required | The tenant felixd lives in |
| `--namespace` | `FELIX_NAMESPACE` | `default` | The namespace. It must already exist |
| `--token-file` | `FELIX_AUTH_TOKEN_FILE` | required | A file holding the token for the brokers |
| `--ca-file` | `FELIX_CA_FILE` | the platform roots | Trust the brokers' certificate from this PEM file |
| `--controlplane-url` | `FELIX_CONTROLPLANE_URL` | required | The control plane's REST address |
| `--controlplane-token-file` | `FELIX_CONTROLPLANE_TOKEN_FILE` | none | A file holding the control plane token. Needed by `run`, which creates streams and caches |

Logs go to standard error and follow `RUST_LOG`. The default is `info` for felixd
and its agent.

### `felixd run`

[Submitting work](/built-on-felix/felixd/submitting-work/) walks through a whole submission.

```bash
felixd run --id counter --input orders --replication-factor 1 counter.wasm
```

It checks that the module is a component of contract 0.2 that no node would refuse,
creates felixd's caches and the workload's `wl.<id>.out` and `wl.<id>.snap`
streams, stores the module in the `modules` cache and writes the spec last. It
prints `workload <id> running module <hash>`. Running it again for an existing id
replaces the spec, and a running workload restarts under it. A new module or input
is refused once the workload has written anything, so use a new id for new code.

| Flag | Default | Meaning |
|---|---|---|
| `--id` | required | The workload's name, used in its stream names. 1 to 64 characters of `a-z`, `0-9` and `-` |
| `--input` | required | The stream the workload reads. It must be a durable stream that already exists |
| `<module>` | required | The workload, a WebAssembly component of contract 0.2 |
| `--snapshot-every` | 1000 | Write a snapshot after this many inputs. At least 1 |
| `--snapshot-every-ms` | none | Also write a snapshot at the first input of every interval this many milliseconds long, by the inputs' append times. At least 1 |
| `--fuel` | 50,000,000 | Fuel for each call into the workload. Stored in the spec |
| `--max-memory` | 64 MiB, in bytes | The most memory the workload may use. Stored in the spec |
| `--snapshot-fuel` | 500,000,000 | Fuel for a whole snapshot or restore. Stored in the spec |
| `--replication-factor` | 3 | Replicas of felixd's caches and the workload's streams. More than 1 makes them `Quorum`. Use 1 against a single broker. A cache that already exists keeps its replicas |

### `felixd agent`

```bash
felixd agent --node-id node-a
```

It watches the `workloads` and `leases` caches, claims the lease of each running
workload nobody holds, runs it while the lease holds, and writes itself to the
`nodes` cache. It runs until Ctrl-C, then releases its leases.

| Flag | Variable | Default | Meaning |
|---|---|---|---|
| `--node-id` | `FELIXD_NODE_ID` | the host name and process id | This node's name in the leases it holds. Each running agent needs its own |
| `--lease-ttl-ms` | | 10000 | How long a lease lives without renewal. At least 2000. Every node in a cluster should use the same value |
| `--max-workloads` | | 64 | The most workloads this node holds at once |
| `--claim-delay-ms` | | 1000 | The longest random wait before claiming a free workload, so the first node to see it does not take everything |

A restarted agent should keep its node id. It does not adopt a lease it finds under
its own name; it waits for the old one to expire.

### `felixd check`

```bash
felixd check --complete
```

| Flag | Meaning |
|---|---|
| `--id` | The workload to check. Repeat for several. Every workload when left out |
| `--complete` | Also report outputs missing from the end of a stream, for when the workloads should have caught up |

It prints `<stream>: <n> records, ok` per workload, or one line per finding, and
exits with 1 if it finds anything. It writes nothing. The findings are described in
[Checking](/built-on-felix/felixd/how-it-works/#checking).

### `felixd replay`

```bash
felixd replay --id counter --to 41
felixd replay --id counter --to 2026-10-29T14:02:00Z
felixd replay --id counter --diff
felixd replay --id counter --diff --module <hash>
```

| Flag | Meaning |
|---|---|
| `--id` | The workload to replay |
| `--to` | An input offset, for the state after that input, or an RFC 3339 time, for the state after every input appended before it. Required unless `--diff` is given, which runs to the last input by default |
| `--diff` | Compare each output the replay produces with the one on the output stream and report the first divergence. Exits with 1 if it finds one |
| `--from` | With `--diff`, the first input to compare. Default 0 |
| `--module` | Run this module, the hash of one in the `modules` cache, over the recorded inputs instead of the workload's own, from its initial state. With `--diff` this checks an upgrade |

Without `--diff` it prints, on standard output, what the workload's `inspect`
returns. A line on standard error says which snapshot it started from and how many
inputs it ran after it. It writes nothing and takes no lease. Until Felix can
report a stream's tail to a reader
([Felix #1621](https://github.com/GetFelix/felix/issues/1621)), it needs publish
grants on the streams it reads, as `check` does.

## What felixd needs from Felix

- **A tenant and a namespace.** felixd creates neither. The namespace must exist
  before `felixd run`.
- **An input stream.** A durable stream, created beforehand. felixd reads it and
  never creates it.
- **Caches and streams.** `felixd run` creates the caches `workloads`, `modules`,
  `leases`, `epochs` and `nodes`, and the two streams per workload, through the
  control plane's REST API. Agents never create anything. The streams have one
  shard each. With a replication factor above 1 they are `Quorum`, so losing one
  broker loses no acknowledged write and stops no claim. Use
  `--replication-factor 1` only against a single development broker.
- **Credentials.** A token for the brokers on every command, and a control plane
  token for `run`. A node reads the `workloads` and `modules` caches, writes
  `leases`, `epochs` and `nodes`, reads every input stream, and reads and publishes
  to `wl.*.out` and `wl.*.snap`. It also writes `workloads`, to record a failure.
  Grants are [RBAC](/features/security/) on the node's token. Today any node may
  write any workload's output, and fencing keeps that correct. Per-lease
  credentials are not built yet ([#74](https://github.com/GetFelix/felixd/issues/74)).
- **A shared felixd build.** Every node of a cluster has to run the same build,
  and nothing enforces it.

## Quickstart

You need Rust and a container runtime. The repository's test stack starts one Felix
broker and control plane with development credentials, and a tenant `dev` with a
namespace `default` and a stream `orders`. It is for development only.

```bash
git clone https://github.com/GetFelix/felixd
cd felixd

docker compose -f tests/felix/compose.yaml up -d --wait
tests/felix/credentials.sh /tmp/felix
export FELIX_BROKERS=127.0.0.1:5000 FELIX_AUTH_TENANT=dev \
  FELIX_AUTH_TOKEN_FILE=/tmp/felix/client.jwt FELIX_CA_FILE=/tmp/felix/broker-cert.pem \
  FELIX_CONTROLPLANE_URL=http://127.0.0.1:8443 \
  FELIX_CONTROLPLANE_TOKEN_FILE=/tmp/felix/controlplane.jwt
```

With Podman, set `COMPOSE="podman compose"` for `credentials.sh` and use
`podman compose` in place of `docker compose`.

**Run the tests first.** The restart test submits the example workloads, kills the
agent mid-stream, restarts it and checks the output stream. It builds the example
workloads itself.

```bash
FELIXD_REQUIRE_FELIX=1 cargo test -p felixd --test restart -- --test-threads=1
```

The failover test runs three agents and six workloads under a steady producer,
kills the live node holding the most leases every 3 seconds and starts it again,
every other time with a fault that also crashes it between an append and its
answer, then checks every output stream. `FELIXD_FAILOVER_SECS` sets how long it
runs, 30 by default:

```bash
FELIXD_REQUIRE_FELIX=1 cargo test -p felixd --test failover -- --test-threads=1
```

**By hand.** A workload is a WebAssembly component. The examples in `workloads/`
build to a core module for `wasm32-unknown-unknown`, which has to be wrapped as a
component. The tests do this with `wit-component`, in
`crates/testkit/src/workloads.rs`, and `wasm-tools component new` does the same
from the command line.

```bash
cargo build --release -p felixd
(cd workloads && cargo build --release --target wasm32-unknown-unknown -p counter)
wasm-tools component new \
  workloads/target/wasm32-unknown-unknown/release/counter.wasm -o counter.wasm

target/release/felixd run --id counter --input orders --replication-factor 1 counter.wasm
target/release/felixd agent --node-id a
```

In another terminal, with the same environment, start a second agent with
`--node-id b`, then publish three records to `orders` with any Felix client. The
`counter` workload counts them:

```bash
target/release/felixd replay --id counter --to 2     # {"count":3}
target/release/felixd check --complete               # wl.counter.out: <n> records, ok
```

Kill the agent that logged `claimed` for the workload. Within one lease TTL, 10
seconds by default, the other agent claims it, restores the latest snapshot, or
starts from the beginning if there is none, and carries on from the right input.
Publish more records and run `felixd check --complete` again: nothing is missing and
nothing is repeated.

## Tests and fault injection

The integration tests set `FELIXD_FAULTS`, a list such as
`crash-before-snapshot=3,drop-input=17`, to crash, freeze, drop or lose the answer
to a particular write. The list is in
`crates/agent/src/faults.rs`. It exists for the tests and is not for production.

`FELIXD_REQUIRE_FELIX=1` makes the integration tests fail instead of skipping when
`FELIX_BROKERS` is not set. Each test creates its own namespace. The three-broker stack for the cluster tests is
`tests/felix/cluster.yaml`.
