# felixctl

The command-line tool for [Felix](https://github.com/GetFelix/felix). It
publishes to and reads from streams, reads, writes and watches cache keys,
shows which broker owns each shard, works consumer groups and counters,
creates, changes and deletes what the control plane manages, manages RBAC
policies and role assignments, moves shards and drains brokers, and runs load
tests.

```bash
brew install getfelix/tap/felixctl          # macOS or Linux, releases after 0.6.0-preview.2
cargo install felixctl --version 0.6.0-preview.3

felixctl context add local --brokers 127.0.0.1:5000 --tenant t1 \
    --token-file token.jwt --ca-file broker-cert.pem
felixctl pub orders 'hello'
felixctl sub orders --from earliest --count 10
felixctl cache get users alice
felixctl group poll orders billing
felixctl topology orders
felixctl stream create orders --shards 4 --replication 3
felixctl stream ls
felixctl bench latency orders
```

Each release also attaches prebuilt archives for Linux, macOS and Windows,
with shell completions and man pages, to its
[GitHub release](https://github.com/GetFelix/felix/releases), and publishes a
container image:

```bash
docker run --rm ghcr.io/getfelix/felixctl:0.6.0-preview.3 --help   # or podman run
```

Releases before 0.6.0-preview.2 are under `ghcr.io/gabloe`, the project's previous owner.

From a checkout, `cargo install --path crates/tools/felixctl`.

Every command prints readable text, or JSON with `--json`, and exits with a
status that says what went wrong: 2 for bad arguments or settings, 3 when
nothing could be reached, 4 when a broker or the control plane refused, 5 when
something does not exist. Deletes and drains ask first on a terminal and need
`--yes` anywhere else. `felixctl help <command>` shows each command's flags and
examples.

Connection settings come from a named context in `felixctl/config.toml` under
the platform config directory, overridden by `FELIX_*` variables, overridden
by flags. The full guide is the
[felixctl page](https://docs.getfelix.dev/getting-started/felixctl/) of the
documentation.

## How it is built

The data-plane commands (`pub`, `sub`, `cache get|put|del|watch`, `group`,
`counter`, `topology`)
use only `felix-client`'s public API, which makes this crate a check that the
API is enough to build tools on. The control-plane commands use the REST API.
`bench` links `felix-loadgen` and runs its scenarios in process, so its
numbers measure what the perf suite measures.

Unit tests cover argument parsing, every command's help text, context
resolution and output formatting (`cargo test -p felixctl --bins`).
`tests/offline.rs` runs the binary without a cluster. `tests/cluster/` runs
every command against brokers and a control plane started by `felix-cluster`;
build the broker first (`cargo build -p felix-broker-service --bin
felix-broker`).

Licensed AGPL-3.0-only, because `bench` links `felix-loadgen`. See
`LICENSING.md` in the repository.
