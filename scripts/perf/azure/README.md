# Azure perf sessions

The automation for `docs/perf-real-network.md`. One *session* = one resource
group = one Bicep deployment, created for a run and deleted after it.

### Secrets: a local `session.env`, never committed

```bash
cp scripts/perf/azure/session.env.example scripts/perf/azure/session.env
# edit session.env — fill in the GUIDs and the ONE client secret
set -a; source scripts/perf/azure/session.env; set +a
```

`session.env` is gitignored (only `session.env.example` is tracked). The
client secret lives there and nowhere else — not in the repo, not in shell
history, not echoed by any script. Everything below reads it from the
environment.

### Orchestration: `az vm run-command`, not SSH

The operator never SSHes into a session. Every operator→VM step — waiting for
cloud-init, seeding, starting brokers, running the matrix, reading RTT — goes
through `az vm run-command invoke`, which rides the Azure control plane over
HTTPS. Two reasons the first live run found the hard way: some operator
networks deep-packet-inspect and reset outbound `:22` to arbitrary cloud IPs
(a fresh Azure VM is not whitelisted the way GitHub is), and Ubuntu 24.04's
socket-activated sshd can fail its first start for want of `/run/sshd`.
run-command sidesteps both and needs no inbound port at all; the VNet-private
control plane is reached by running the seed *on* the loadgen, which is inside
the VNet. The NSG still allows SSH from your address so a human *can* open a
shell to debug, but nothing in the automation depends on it. Scripts shipped to
run-command execute under **dash as root**, so `seed-remote.sh` and the inline
snippets are POSIX sh — no `pipefail`, arrays, or `[[ ]]`.

### Run

```bash
az login                                    # the Azure account
# session.env is already sourced (above), so the suite has everything: the
# subscription, the tier/release, and the IdP secrets it mints a token from.
./session.sh                   # provision + seed (~10 min; up to ~60 with BROKER_REFS)
./run.sh                       # the matrix (~2-4 h); results in sessions/<name>-results/
./teardown.sh                  # ALWAYS. ~$1/hour while it exists.
```

(The manual path still works: set `IDP_TOKEN` plus `IDP_JWKS_URL` yourself
and skip the three secrets. The issuer and audience are read off the token.)

- **Tiers**: `t1` one-zone proximity-placed baseline; `t2` brokers across
  zones 1/2/3 (set `REPLICATION_FACTOR=3` in the environment before
  `session.sh` so the seeded streams replicate). t3 is a second small
  deployment of a loadgen in another region, pointed at a t1 session.
- **Seeded streams**: `perf`, `perf-durable`, `perf-quorum` and
  `perf-durable-quorum` — the two durability settings crossed with the two
  consistency levels, so one seed covers every combination a run wants to
  price. Pass the one you want as `--stream`. `Quorum` only means anything
  above `REPLICATION_FACTOR=1`: a quorum of one is the leader, so on a t1
  session the `-quorum` streams measure the same path as their siblings.
- **IdP**: an Entra **app registration** (no user — a perf harness wants a
  non-interactive credential), used through the **client-credentials** grant.
  `idp-token.sh` turns `IDP_TENANT_ID` + `IDP_CLIENT_ID` + `IDP_CLIENT_SECRET`
  + `IDP_SCOPE` into an access token; the app-only token's `sub`/`oid` is
  the service-principal identity Felix authorizes. `IDP_SCOPE` says what to
  *request* — the Application ID URI or the bare client id, both work — and is
  not the audience the token comes back with: the same credential issues a v1
  token (`aud=api://<client-id>`) or a v2 one (`aud=<client-id>`) depending on
  the app, so `seed-remote.sh` registers the `aud` and `iss` it reads off the
  minted token rather than either being assumed. Registering a requested value
  the token does not carry is a `401 invalid token` on every exchange, and
  bootstrap-initialize is exactly-once, so correcting it afterwards needs the
  control plane's store cleared. Measuring the real exchange is the point, so the
  seed and the exchange-latency scenario both run against this, never demo
  auth.
- **What runs where**: with `BROKER_REFS` set, generator 0 builds each ref
  (resolved to a full SHA) during provisioning and serves the tarballs on
  `:8088` inside the VNet; `seed.sh` installs every build under
  `/opt/felix/<name>/` and links `ACTIVE_REF` (default: the first) as
  `/usr/local/bin/felix-broker`. Without it, brokers and the control plane run
  `RELEASE_URL` or the `RELEASE_TAG` release. Every generator **builds**
  `felix-loadgen` from `LOADGEN_REF` (default `main`) once, before any run.
- **Budget guardrails**: everything is in the one group; `teardown.sh`
  deletes it; `session.sh` stamps an `autoTeardownAfter` tag at +8h as the
  backstop for a wedged session (enforce it with a subscription automation
  rule, or just check the tag when you log in).
- **Honesty rules**: compare only within a session; keep the machines
  otherwise idle during `run.sh`; every published number cites the
  `session.json` beside it.

### The v0.6.0 campaign: sessions A, B and C

One driver per session runs its whole matrix unattended and writes
`sessions/<name>-results/`. Re-running a driver resumes: finished cells are
kept (`RESUME=0` redoes them). The three sessions are separate resource groups,
so they can run at the same time in different regions, each against that
region's vCPU quota (A needs about 30, B and C about 22).

```bash
# A: one L8as_v4 NVMe broker, four generators. #557/#559 listener sweep, #547 on NVMe.
SESSION=v060-a LOCATION=eastus2 BROKER_COUNT=1 BROKER_VM_SIZE=Standard_L8as_v4 \
  USE_LOCAL_NVME=true LOADGEN_COUNT=4 LOADGEN0_VM_SIZE=Standard_D8as_v5 SHARDS=48 \
  BROKER_REFS="main 8f1736eb" FP_REFS=main ./session.sh
SESSION=v060-a ./session-a.sh

`CONTROLPLANE_VM_SIZE` (default `Standard_D2as_v5`) moves the control plane to another VM family when the
Dasv5 family quota (20 vCPUs by default) is taken by brokers and generators, e.g. `Standard_D2s_v5`.

# B: three D4as_v5 brokers on Premium SSD. #375 rows, #547 on slow storage, RF=1 for #425.
SESSION=v060-b LOCATION=westus3 BROKER_COUNT=3 LOADGEN_COUNT=2 SHARDS=12 \
  BROKER_REFS="main 8f1736eb" ./session.sh
SESSION=v060-b ./session-b.sh

# C: three brokers in zones 1/2/3, RF=3, generators and control plane in zone 1. #425.
SESSION=v060-c LOCATION=centralus TIER=t2 REPLICATION_FACTOR=3 BROKER_COUNT=3 \
  LOADGEN_COUNT=2 SHARDS=12 BROKER_REFS=main ./session.sh
SESSION=v060-c ./session-c.sh
```

`STEPS` picks parts of a driver (for example `STEPS="smoke sweep"`), `TRIALS`
sets trials per cell (default 3). The header of each driver lists its cells.

Ingest cells run for `CELL_SECS` seconds (default 90), not a record count, so
a 256 B unbatched cell lasts as long as a 4 KiB x 64 one. Every generator in an
ingest cell starts publishing at the same wall-clock time, `START_DELAY_SECS`
(default 45) after launch, so the steady-state window covers the whole run.
The `shapes` step of each driver crosses payload (`SHAPE_PAYLOADS`, 256 1024
4096), batch (`SHAPE_BATCHES`, 1 64) and in-flight acked batches per publisher
(`SHAPE_IN_FLIGHT`, 0 64; 0 is fire-and-forget), `SHAPE_TRIALS` times (default 1).
An acked cell is what a client that waits for its acks gets; on a durable
stream with `FELIX_ACK_ON_COMMIT=1` that is the durable rate.

Session C runs its steps twice: under the lease, then after its `lease-free`
step finalizes `generation_start`, `majority_ack` and `lease_free_reads` (cells
tagged `-lf`). Finalizing is one-way, so a re-run of the lease cells needs a
fresh session.

**Knobs.** Brokers read `/etc/felix/overrides.env` after the regenerated
`broker.env`, so it wins. Every session starts from the calibrated base in
`lib.sh` (`base_overrides`: `FELIX_ACK_ON_COMMIT=1`, `FELIX_STORAGE_IO_URING=1`,
`FELIX_IO_RUNTIME_THREADS=0`, the listener count, periodic fsync); cells add to
it. By hand:

```bash
SESSION=v060-a ./broker-env.sh show
SESSION=v060-a ./broker-env.sh set FELIX_QUIC_LISTENERS=4    # restarts, waits for /ready
SESSION=v060-a ./broker-env.sh reset
```

`EXTRA_BROKER_ENV="K=V ..."` and `EXTRA_LOADGEN_ENV="K=V ..."` apply to every
cell of a driver run; add `RUN_TAG=<tag>` so those cells land beside the
defaults instead of being skipped as done. A sweep is one line:

```bash
SESSION=v060-a ./sweep.sh --name flushconc --durable \
  FELIX_BROKER_PUB_FLUSH_CONCURRENCY=16,32,64 client:FELIX_PUB_CONN_POOL=4,8 -- \
  --scenario ingest --stream perf-durable --payload-bytes 4096 --batch 64 \
  --concurrency 16 --duration-secs 90 --keys 48
```

**Any build, hot-swapped.** `deploy-ref.sh` builds a branch, tag or SHA on
generator 0 (15-25 min; it refuses while a load is running there) and installs
it on every broker; `--activate` switches to it, `--fp` builds with frame
pointers. Switching between installed builds is `broker-env.sh activate <label>`.

**Profiles.** `PROFILE=1` (every cell) or `PROFILE_CELLS=<regex>` records
`perf record -F 199 -g` and `pidstat -t` on each broker during the cell, and
brings home a per-thread CPU table, the heads of the perf report and the
collapsed stacks (`<broker>.folded.gz`, flamegraph.pl input). Stacks resolve
on a `<ref>-fp` build (`FP_REFS=main` at provision, or `deploy-ref.sh --fp`);
the Rust standard library itself is still built without frame pointers.

**What a cell records** (`cells/<name>/`): the broker's running binary (sha256
and git SHA) and full `FELIX_*` environment; counters before and after
(append bytes and records, sync count and duration, group-commit fan-in,
quorum failures, UDP `RcvbufErrors`/`InErrors`, datagrams per listener port);
a 1 Hz sampler's CPU and append-rate summary on every broker and generator,
and each broker's raw 1 Hz series (`<broker>.series.tsv`: append and publish
bytes, bytes per listener port, UDP datagrams and `RcvbufErrors`, process CPU
ticks); the instrument's output, with wall-clock `gen.start`/`gen.end` around
it. `summarize.py` writes `cells.csv` and `summary.md`.
**Steady-state broker append MB/s is the throughput number**: the median
one-second rate, summed over brokers, over the window when every generator was
running, less its first and last 10%. Generators do not start together, finish
together or get equal shares, so summing their averages or dividing a
before/after delta by the cell's length both miss by a wide margin; those older
figures stay in the table for comparison, and cells recorded before the series
existed are marked `legacy`. Fire-and-forget publishes also make the client's
figure an enqueue rate. Durable cells start from a wiped
`/data/felix` and dropped page cache (`WIPE_DURABLE=1`; off in session C, where
a whole-cluster wipe under live replicas is its own experiment). `fio-baseline.sh`
takes the device baseline; the drivers run it first.

### What is committed, and what is not

`sessions/<name>-results/` and `sessions/*-findings.md` **are tracked**. They
are the evidence behind every performance figure the docs publish, and a number
whose working lives on one laptop is a number nobody can check.

`sessions/<name>.env` is **not**: it holds the bootstrap token and the
addresses of a live cluster. That is the only thing the ignore rule excludes,
so adding a run's output is just `git add`.

Results are loadgen stdout, a JSONL of `LOADGEN_JSON` rows, and the
`session.json` describing the hardware. No script writes a credential there —
keep it that way, and check before committing a session that used a new script.

### Script modes

Every script meant to be *run* is executable. `lib.sh`, `cells.sh`,
`nats/nats-lib.sh`, `seed-remote.sh`, `remote/felix-agent.sh`,
`nats/remote/nats-agent.sh` and `cloudinit/provision-loadgen.sh` are
deliberately not: the first three are sourced, and the rest are shipped to a
VM (by run-command or cloud-init) and run there.

### NATS comparison on session A's VMs

`nats/` runs NATS JetStream on the same machines as session A, after the
Felix cells, and writes its cells beside them (`cells/nats-*`), so one
`summarize.py` table holds both. Nothing is provisioned: the broker VM's
`felix-broker` is stopped (not removed) while NATS runs, and started again
at the end.

```bash
SESSION=v060-a ./nats/install.sh           # nats-server + CLI, TLS, TCP limits
SESSION=v060-a STEPS=sweep ./nats/nats-cells.sh    # find NATS's best shape (~2.5 h)
# Re-run the matrix with the best shape the sweep found, e.g.:
SESSION=v060-a STEPS=matrix RUN_TAG=best NATS_STREAMS=12 PUBS_PER_GEN=32 \
  ASYNC_WINDOW=1024 NATS_SERVER_ENV="GOGC=400" ./nats/nats-cells.sh     # ~4 h
SESSION=v060-a ./nats/uninstall.sh         # stop NATS, wipe /data/nats, Felix back
```

Versions are pinned in `nats/nats-lib.sh` and checked against each release's
`SHA256SUMS`: nats-server 2.15.0 and natscli 0.5.0, the latest stable releases
at the time of writing. Override with `NATS_VERSION`/`NATS_SHA256` and
`NATS_CLI_VERSION`/`NATS_CLI_SHA256`.

**What a NATS cell records**, in the layout `cells.sh` uses: `meta.env` (the
shape in felix-loadgen's flag names so the columns line up, plus `nats_cmd`,
the exact `nats bench` command line); the server's `before.txt` (version and
binary sha256, `GOMAXPROCS`, cores, `max_payload`, `max_pending`,
`write_deadline`, every line of `nats.conf`, the `GO*` environment, the
`/data` mount, kernel sysctls, NIC MTU and CPU count); each generator's
`armed.txt` (its sysctls, MTU and CPU count); before/after counters; the
1 Hz series from `nats-server`; and each generator's `run.txt` with a
`NATS_BENCH_JSON` line and `gen.start`/`gen.end`. The steady-state rate is
computed exactly as for Felix: median one-second rate over the all-generators
window, trimmed 10% each end.

For NATS, *append* is records appended (the sum of the streams' last
sequence numbers) times the mean stored record size from `/jsz`. That keeps
counting when a memory stream discards at its limit; for file streams it equals
the growth of JetStream's stored bytes. Stored size includes NATS's per-record
overhead (subject, header, sequence and timestamp), as Felix's append bytes
include its record header. *Ingress* is `/varz` `in_bytes`, all bytes the
server received, protocol included.

**Cells** (`nats-cells.sh`): `matrix` runs each pair `TRIALS` times (3);
`sweep` varies one knob at a time around the base shape. Session A's `shapes`
cells spread over the stream's 48 shards and its listener-sweep cells over 12
keys; the matrix uses 12 streams, and the sweep includes 48.

| Felix cell in session A | NATS cell | What matches |
|---|---|---|
| `a-shape-dur-p{4096,256}-b1-f64` (acked, on_commit) | `nats-js-async-always-p*-w64` | 64 single-record publishes outstanding per publisher, fsync before ack |
| same | `nats-js-fast-always-p*-f1-w64` | the same with a sliding window (fast batch, one ack per record, 64 outstanding) |
| `a-shape-dur-p{4096,256}-b64-f64` | `nats-js-fast-always-p*-f64-w64` | 64 records per ack, 64 acks outstanding (4096 records in flight) |
| `a-shape-inmem-*-f64` | `nats-js-*-memory-*` | acked publish into an in-memory stream |
| Felix periodic fsync (`base_overrides`; no session A cell) | `nats-js-*-default-*` | written to the page cache, fsynced later (NATS: every 2 min) |
| `l557-l4-io0-inmem`, `a-shape-inmem-p4096-b64-f0` (fire-and-forget) | `nats-core-p4096` | no ack; NATS core publish to subjects without subscribers |

`always` is `jetstream { sync_interval: always }`: an fsync on every write,
the pair for Felix's `FELIX_DURABLE_FSYNC_MODE=on_commit` with
`FELIX_ACK_ON_COMMIT=1`. `default` leaves `sync_interval` unset, which is
two minutes in 2.15 (`defaultSyncInterval` in `server/filestore.go`).

#### Fairness

The comparison is only worth publishing if NATS gets the same machine and its
own best configuration. Where something could not be matched, it is
disclosed below rather than chosen.

Matched:

- **Hardware.** The same broker VM (Standard_L8as_v4, 8 vCPU) and the same
  `/data` NVMe RAID0; the same four D4as_v5 generators in the same VNet. The
  Felix broker is stopped during NATS cells, so nothing else shares the cores
  or the disks.
- **Starting state.** Every cell restarts `nats-server` on an empty
  `/data/nats` with the page cache dropped, as every Felix durable cell starts
  from a wiped `/data/felix`.
- **Load.** 4 generators x `PUBS_PER_GEN` (16) publishers, the payload sizes
  Felix used (4096 and 256 B), all generators starting at one wall-clock time
  `START_DELAY_SECS` after launch, publishing for `CELL_SECS` (90 s), 3 trials.
- **Spread.** 12 R1 streams (`NATS_STREAMS`) for Felix's 12 keys. Async and
  core publishers rotate their subjects over every stream
  (`--multisubject --multisubjectmax 12`), as a Felix publisher rotates over
  its keys.
- **Durability pairs and replication.** R1 streams for Felix's RF=1;
  `always` for OnCommit, `memory` for in-memory, `default` for periodic.
- **Encryption.** NATS clients connect over TLS (`tls://`, P-256 server
  certificate from a throwaway CA), as Felix clients always do over QUIC.
- **Kernel tuning.** Felix's VMs allow 25 MiB UDP socket buffers. NATS gets
  the TCP equivalent on the broker and the generators: `tcp_rmem`/`tcp_wmem`
  maximum 25 MiB, `somaxconn` 4096, `tcp_slow_start_after_idle=0`
  (`nats-agent tune`; `uninstall.sh` restores the originals). The NIC MTU is
  not changed for either system, and is recorded in every cell.
- **Measurement.** The same sampler on `nats-server` (process CPU, system
  CPU), the same steady-state window, the same summary.
- **Best configuration, not defaults.** The `sweep` step tries stream count,
  async window, publishers per generator, publish API (sync, async, fast batch
  with several flow sizes) and `GOGC`, in both file-store modes. The quoted
  NATS rows should be the matrix re-run at the best shape (`RUN_TAG=best`),
  just as Felix is quoted at its best listener and client settings. Server
  settings can be swept further with `NATS_SERVER_ENV` (process environment)
  and `NATS_EXTRA_CONF` (server config lines). `GOMAXPROCS` is left at the Go
  default, every core, and recorded.

Cannot be matched, and how each is handled:

- **Transport.** Felix speaks QUIC over UDP; NATS speaks its text protocol
  over TCP. Kernel limits are matched as above; the protocols are not. Felix
  cells report UDP receive-buffer drops; NATS cells record the TCP counters
  (`tcp.*`, including retransmits) instead.
- **Batching and ack semantics.** A Felix batch is one publish of up to 64
  records with one ack, and `--in-flight` is a sliding window of batches.
  `nats bench js pub async --batch 64` sends 64 messages, then waits for all
  64 acks before sending more, so its window drains to zero every round. Fast
  batch publish (2.14+) is the sliding-window equivalent, so the matrix runs
  both. A fast batch targets one stream, so fast-batch publishers are pinned
  to stream `(publisher index) mod 12` instead of rotating, which gives four
  streams six publishers and eight streams five.
- **Fire-and-forget.** Felix's fire-and-forget publish still appends to the
  stream. Core NATS publish to a subject without subscribers is routed and
  dropped; there is no JetStream equivalent without an ack. Its row is
  quoted by ingress, and the append column reads zero.
- **Run length.** `nats bench` stops on a message count; its `--duration`
  needs `--throughput`, a rate cap. Each generator therefore runs `nats bench`
  in chunks of about `CHUNK_SECS` (10 s), each sized from the previous chunk's
  rate, until the cell's end. The reconnect between chunks takes a fraction of
  a second, shows up as a dip in a few one-second samples, and the median
  ignores it. The cost falls on NATS, so it is a disadvantage, not an
  advantage; lengthen `CHUNK_SECS` to shrink it.
- **Latency.** `nats bench` reports latency per operation: one async window,
  one sync publish, one fast-batch ack. Felix reports it per batch ack. The
  `p50 / p99` column is not comparable across the two and should not be
  quoted side by side.
- **Implementation.** Go (garbage collected; `GOGC` is swept) against Rust.
  Nothing to match; noted.
- **Memory streams.** A 90 s cell does not fit in RAM, so memory streams are
  limited to their share of 90% of `max_memory_store` (75% of RAM) and
  discard their oldest records; append is counted from sequence numbers so
  the discards do not lower it.
