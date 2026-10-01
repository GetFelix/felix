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
Felix cells, and writes its cells beside them (`cells/nats-*`, `cells/ab-*`),
so one `summarize.py` table holds both. Nothing is provisioned: the broker
VM's `felix-broker` is stopped (not removed) while NATS runs, and started again
at the end.

```bash
SESSION=v060-a ./nats/install.sh                   # nats-server + CLI, TLS, TCP limits
SESSION=v060-a STEPS=sweep ./nats/nats-cells.sh    # NATS's best shape (47 cells)
# The matrix at the best shape the sweep found, e.g.:
SESSION=v060-a STEPS=matrix RUN_TAG=best FAST_WINDOW=256 PUBS_PER_GEN=32 ./nats/nats-cells.sh
SESSION=v060-a FAST_WINDOW=256 ./nats/interleave.sh   # Felix and NATS cells alternated
SESSION=v060-a ./nats/nats-latency.sh             # publish latency, Felix and NATS alternated
SESSION=v060-a ./nats/uninstall.sh                 # stop NATS, wipe /data/nats, Felix back
```

Versions are pinned in `nats/nats-lib.sh` and checked against each release's
`SHA256SUMS`: nats-server 2.15.0 and natscli 0.5.0, the latest stable releases
at the time of writing. Override with `NATS_VERSION`/`NATS_SHA256` and
`NATS_CLI_VERSION`/`NATS_CLI_SHA256`.

**How a NATS cell runs.** Each shape is calibrated once: a
`NATS_CALIB_SECS` (15 s) run on all four generators, its rate read from the
server's counters. Each generator then runs one continuous `nats bench` per
process, sized to 1.5x that rate over `CELL_SECS` + 5 s, starting at the
cell's shared wall-clock start and interrupted with `SIGINT` (`timeout -s
INT`) at its end; `gen.end` is the interrupt time. `nats bench` can only stop
on a message count (its `--duration` needs `--throughput`, a rate cap), and it
prints nothing when interrupted, so throughput comes from the server's
counters over the steady-state window, exactly as Felix's does. A cell whose
count ran out before the end fails, and so does a cell where any VM's NIC MTU
differs from the Felix cells'. A generator above `NATS_GEN_BUSY_MAX` (85%)
CPU reruns the cell with twice the processes per generator, up to
`NATS_MAX_PROCS` (4); still saturated, the cell is flagged
`flag.gen_saturated` in its `meta.env`. Generator CPU sums every `nats`
process.

**What a NATS cell records**, in the layout `cells.sh` uses: `meta.env` (the
shape in felix-loadgen's flag names so the columns line up, messages per
client, processes, the exact `nats bench` command line, the expected MTU,
generator CPU); the server's `before.txt` (version and binary sha256,
`GOMAXPROCS`, cores, `max_payload`, `max_pending`, `write_deadline`, every
line of `nats.conf`, the `GO*` environment, the `/data` mount, kernel
sysctls, NIC MTU, CPU count); each generator's `armed.txt` (its sysctls, MTU
and CPU count); before/after counters, including the TCP counters; the 1 Hz
series from `nats-server`; and each generator's `run.txt`. Felix snapshots
now record `nic.mtu` too.

For NATS, *append* is records appended (the sum of the streams' last
sequence numbers) times the mean stored record size from `/jsz`. It keeps
counting when a memory stream discards; for file streams it equals the growth
of stored bytes. A failed `/jsz` scrape is recorded as `NA`, never 0, and is
left out of the rates. *Ingress* is `/varz` `in_bytes`, protocol included.
The summary's **records/s** and **payload MB/s** divide the steady append
rate by the mean appended record size and multiply by the payload, on both
systems. Compare on those: a NATS record stores 30-40 B of subject and
metadata beside the payload, and Felix's has its own header.

**Cells.** The `matrix` crosses durability mode (`always`, `memory`,
`default`), payload (4096, 256 B) and stream count (12 and 48, the key counts
of session A's listener-sweep and shape cells, and 64, one stream per
publisher, as a NATS-only extra), 3 trials each. Each system is quoted at its
best stream or key count.

| Felix cell in session A | NATS cell | What matches |
|---|---|---|
| `a-shape-dur-p*-b1-f64` (acked, on_commit) | `nats-js-fast-always-p*-f1-w64-*` (lead) | a sliding window of 64 single-record publishes per publisher, each acked after fsync |
| same | `nats-js-async-always-p*-w64-*` (secondary) | 64 publishes, then a wait for all 64 acks (stop-and-wait) |
| `a-shape-dur-p*-b64-f64` | `nats-js-fast-always-p*-f64-w64-*` | 64 records per ack, 64 acks outstanding |
| `a-shape-inmem-*-f64` | `nats-js-fast-memory-*` | acked publish into an in-memory stream |
| Felix periodic fsync (`base_overrides`; no session A cell) | `nats-js-fast-default-*` | page cache, fsynced later (NATS: every 2 min) |
| `l557-l4-io0-inmem`, `a-shape-inmem-p4096-b64-f0` | `nats-core-p4096-*` | no ack; core publish to subjects without subscribers |

`always` is `jetstream { sync_interval: always }`, the pair for
`FELIX_DURABLE_FSYNC_MODE=on_commit` with `FELIX_ACK_ON_COMMIT=1`. `default`
leaves `sync_interval` unset: two minutes in 2.15 (`defaultSyncInterval` in
`server/filestore.go`).

The `sweep` runs stream count x fast-batch window (12/24/48/64 x
16/64/256/1024/4000) in `always` mode, then one knob at a time in `always` and
`default`: publishers per generator, fast-batch flow, async window, sync
publish and `GOGC`. `interleave.sh` alternates a Felix best-profile cell and
its NATS pair, `AB_TRIALS` (4, must be even) times: Felix first on odd
trials, NATS first on even ones (AB BA AB BA), so drive wear and time of day
fall on both alike and neither system always runs first.

#### Fairness

The comparison is only worth publishing if NATS gets the same machine and its
own best configuration. Where something could not be matched, it is
disclosed below rather than chosen.

Matched:

- **Hardware.** The same broker VM (Standard_L8as_v4, 8 vCPU) and the same
  `/data` NVMe RAID0; the same four D4as_v5 generators in the same VNet. The
  Felix broker is stopped during NATS cells and NATS during Felix cells.
- **Starting state.** Every NATS cell starts on an empty `/data/nats`, with
  `fstrim /data` and the page cache dropped. Felix durable cells wipe
  `/data/felix` and drop the cache but do not trim; the cells in
  `interleave.sh` trim before both systems, so quote those when the drives'
  state could matter.
- **Load.** 4 generators x `PUBS_PER_GEN` (16) publishers, the payload sizes
  Felix used, all generators starting at one wall-clock time, publishing for
  `CELL_SECS` (90 s), 3 trials, rate taken by the same steady-state rule.
- **Spread.** Stream count is a matrix dimension matched to Felix's key count
  (12, 48), plus 64. Under `sync_interval: always` an R1 file stream fsyncs
  each write under its own lock, so NATS's durable throughput grows with
  streams; it is quoted at its best count, as Felix is at its best.
- **Durability pairs and replication.** R1 streams for Felix's RF=1;
  `always` for OnCommit, `memory` for in-memory, `default` for periodic. Both
  systems ack an `always`/OnCommit publish only after its fsync.
- **Memory streams.** Felix's in-memory stream keeps the newest 1024 records
  per shard (`DEFAULT_LOG_CAPACITY`, no env override) and drops the oldest.
  NATS memory streams keep the same total, `1024 x SHARDS` records split
  over the streams (`--max-msgs`, `--discard old`), so both drop the oldest at
  the same depth. `GOMEMLIMIT` is set to 85% of RAM so the Go runtime has a
  memory target.
- **Encryption.** NATS clients connect over TLS (P-256 server certificate),
  as Felix clients do over QUIC.
- **Kernel and NIC.** Felix's VMs allow 25 MiB UDP socket buffers. NATS gets
  the TCP equivalent on the broker and the generators: `tcp_rmem`/`tcp_wmem`
  maximum 25 MiB, `somaxconn` 4096, `tcp_slow_start_after_idle=0`. These stay
  set during interleaved Felix cells; they do not touch UDP. The NIC MTU is
  set to the one the Felix cells recorded (or `NATS_MTU`), checked end to end
  with a don't-fragment ping, recorded per cell, and a mismatch fails the cell.
- **Failed scrapes.** A failed metrics scrape on either system is recorded
  as `NA` and skipped; a cell with more than 5% of them is flagged in the
  summary (`ss_na_pct`), since the rate interpolates across the gap.
- **Measurement.** The same sampler (all `nats-server` threads; every `nats`
  process on a generator), the same window, the same summary, compared on
  records/s and payload MB/s.
- **Best configuration, not defaults.** The sweep above; `NATS_SERVER_ENV`
  (process environment) and `NATS_EXTRA_CONF` (server config) sweep further.
  `GOMAXPROCS` is left at the Go default, every core, and recorded.

Cannot be matched, and how each is handled:

- **Transport.** QUIC over UDP against NATS's protocol over TCP. Kernel limits
  are matched as above; the protocols are not. Felix cells report UDP receive
  drops, NATS cells the TCP counters (`tcp.*`, retransmits included).
- **Batching and acks.** A Felix batch is one publish with one ack and its
  in-flight window slides. NATS fast batch (2.14+) slides too and leads the
  pairing; `nats bench js pub async --batch N` sends N and waits for all N
  acks, so its window drains every round, and it is a secondary row. nats.go
  limits async publishes in flight to 4000 (`PublishAsyncMaxPending`), which
  nats bench does not raise, so async windows stop at 4000. A fast batch
  targets one stream, so fast-batch publishers are pinned to stream
  `index mod streams` instead of rotating.
- **Group commit.** Under `always`, NATS fsyncs per write per stream. Felix
  group-commits: one fsync serves every waiting publish. Both ack after the
  fsync; how many fsyncs that costs is the design difference being measured.
- **NATS's own recommendation.** NATS recommends R3 with the default sync
  interval for durability, not R1 with `sync_interval: always`. R1 `always` is
  run because it is the like-for-like pair for Felix's RF=1 OnCommit; the
  `default` rows show R1 under NATS's default sync. R1 streams also offer
  `persist_mode: async`, not run here.
- **Fire-and-forget.** Felix's fire-and-forget publish still appends. Core
  NATS publish to a subject without subscribers is routed and dropped. Its row
  is quoted by ingress records/s; the append column reads zero.
- **Latency.** Measured by `nats-latency.sh`, not by `nats bench`. See
  "Latency" below.
- **Authentication.** Felix clients present a JWT the broker verifies at
  connect; NATS runs without auth. A connect-time cost, outside the window.
- **TLS key exchange.** Felix's QUIC stack and Go's TLS may negotiate
  different key exchanges (Go's default includes hybrid X25519+ML-KEM). A
  one-off cost per connection, outside the steady-state window.
- **Deduplication.** Publishes carry no `Nats-Msg-Id`, so the stream's
  duplicate window costs nothing.
- **Implementation.** Go (garbage collected; `GOGC` is swept) against Rust.
  Nothing to match; noted.

#### Latency

`nats/nats-latency.sh` builds `nats/latency/` on generator 0
(`install-latency.sh`, the same fetch-and-build path as `deploy-loadgen.sh`)
and runs it beside felix-loadgen's own latency cells. `nats-latency` is a port
of felix-loadgen's `pubsub` scenario at batch 1, so both tools measure the
same thing the same way:

- One publisher, one subscriber on a second connection, both on generator 0.
- Closed loop with one publish in flight: the next publish starts when the
  last one is acknowledged. 2000 warmup publishes are discarded and 20000 are
  measured, at payloads 0, 256, 1024 and 4096 B. As in felix-loadgen, a
  payload under 16 B grows to 16.
- Before the subscriber exists, 50 acknowledged publishes in a row prove the
  server is ready. The subscriber then starts at the live tail.
- Ack latency is the time from just before the publish call to its
  acknowledgement. Delivery latency is the subscriber's receive time minus a
  timestamp the payload carries in its first 16 bytes (sequence number, then
  nanoseconds since the process started). Both ends read the same monotonic
  clock (`Instant`) in one process, so no clock sync is involved.
- Percentiles come from the full sorted list of microsecond samples, at
  index `round((n - 1) * q)`. No histogram. `stats.rs` and `framing.rs` are
  copied from felix-loadgen unchanged.
- The output is a `LOADGEN_JSON` line with felix-loadgen's field names
  (`ack_latency_us`, `delivery_latency_us`, each with `p50`, `p99`, `p999` and
  `max`), so `summarize.py` puts both in the same columns.

| Pair | Felix cell | NATS cell |
|---|---|---|
| `oncommit` | `perf-durable`, `on_commit` fsync | JetStream file stream, `sync_interval: always` |
| `periodic` | `perf-durable`, `periodic` fsync | JetStream file stream, default sync |
| `inmem` | `perf`, in-memory | JetStream memory stream |
| `core` | the `inmem` cell | core NATS publish and subscribe, no stream |

Cells are `felix-lat-<pair>-p<size>-t<n>` and `nats-lat-<pair>-p<size>-t<n>`.
Each trial runs Felix and NATS back to back, Felix first on odd trials and
NATS first on even ones. With the default `TRIALS=3`, Felix goes first twice.
Every cell starts from a wiped, trimmed store, with the other system stopped
and the NIC MTU the Felix cells recorded. NATS connects over TLS with the
`install.sh` CA. The JetStream streams are R1, as Felix runs RF=1.

Why the NATS side is not handicapped:

- Both systems ack only once the record is stored at the pair's durability
  level. With `sync_interval: always`, NATS fsyncs before it acks, and Felix
  with `on_commit` and `FELIX_ACK_ON_COMMIT=1` does the same.
- Delivery goes through an ordered push consumer (`DeliverPolicy::New`, no
  acks, flow control), the lowest-latency JetStream consumer. JetStream
  delivers a message only after storing it, just as Felix fans out after the
  append commits. A core subscription on the stream's subject would get the
  message before it was stored. That would be faster, but it would not be the
  same measurement.
- `core` gives NATS its lightest path. Core publish has no ack, so the loop
  waits on a flush (PING/PONG) instead. That is one round trip, and nothing
  is stored. Read it as a floor, not as a pair for Felix's acked publish.
- The client is async-nats 0.50.0, pinned, on the same tokio multi-threaded
  runtime and with the same release profile (fat LTO, one codegen unit) as
  felix-loadgen. The ack timeout is 30 s, the same as felix-client's
  `ACK_WAIT_TIMEOUT`, so a slow fsync waits rather than erroring.

Shared limits:

- Neither tool has an open-loop (fixed-rate) mode, so both have the same
  coordinated-omission blind spot. A stall delays the next publish instead of
  queueing more, which hides queueing delay from p99 and above. The
  comparison is still like for like, but neither tail is what a client
  publishing at a fixed rate would see.
- felix-loadgen retries a publish that hits a routing transient and samples
  only the successful attempt (`publish_retries`). A single NATS server has
  no such transient, so `nats-latency` fails on any publish error and always
  reports `publish_retries: 0`.
