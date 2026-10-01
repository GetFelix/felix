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

**Session credentials stay current by refresh, not by a long TTL.** Access
tokens live 8 h. `seed.sh` gives every broker its own exchange, written as
`/etc/felix/node.token` and `/etc/felix/node.refresh_token`; the broker
refreshes before expiry and writes the rotated refresh token back, and
`felix-agent restart` renews `node.token` while the broker is stopped.
Refresh tokens rotate and presenting a spent one revokes its whole chain, so
no two holders share one. The generators' client token has its own chain on
generator 0 (`~felix/felix-session/refresh`): each driver refreshes it at
start, a cell again once six hours have passed, and the result is copied to
the other generators.

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
`seed-remote.sh`, `remote/felix-agent.sh` and `cloudinit/provision-loadgen.sh`
are deliberately not: the first two are sourced, and the rest are shipped to a
VM (by run-command or cloud-init) and run there.
