# Sharding investigation: keyed publishes, io_uring, and a ceiling that would not move

Working log of a performance session on Azure local NVMe. Records what was
measured, what shipped, which hypotheses turned out to be wrong, and the bugs
in the *instrument* that made two of those hypotheses look tested when they
were not.

**Status: two fixes shipped, one ceiling open.** The routing-key penalty is
gone (#549, merged in #551). `io_uring` device flushes are available behind a
flag (#548). Sharding on a single broker is established as *not* a throughput
lever and is documented as horizontal-only (#552). What remains unexplained is
a per-broker plateau at **~900 MB/s** with roughly a third of the broker's CPU
idle, the generators three-quarters idle, and the device at ~67% of its
measured capability. Ten hypotheses were eliminated, and the eleventh (a single
saturated task at the quinn endpoint) was **measured and supported**: the
endpoint driver's receive path is ~88% of one core, on a task that cannot use
more than one. Throughput scales **2.1x with a second broker**, which is the
same finding from the other direction. Section 9 has the profile.

Map of this document: section 1 is the rig. Sections 2–3 are the two shipped
changes and their measurements. Section 4 is the run matrix. Section 5 is the
elimination history for the ceiling. Section 6 is what sharding actually costs.
Section 7 is the roadblocks, including the wrong conclusions drawn along the
way. Section 8 is what is still open. Section 9 is the two-broker session that
answered it.

---

## 1. The rig

One `L8as_v4` broker (8 vCPU, 4× NVMe in RAID0, ext4 on `/dev/md0`), four
`D4as_v5` generators (4 vCPU each), one control plane, all inside one Azure
VNet. 4 KiB payloads, batch 64, `AckMode::None`, `FELIX_DURABLE_FSYNC_MODE` set
to `on_commit`, real Entra tokens over TLS/QUIC, replication factor 1.

Storage was wiped between every run. This was not ceremony: the volume had
accumulated **396 GB** from prior sessions, and stale data had confounded an
earlier investigation.

SSH is DPI-blocked in this environment. Every remote action goes through
`az vm run-command`, which has seconds of dispatch latency. That shapes
how CPU is sampled (section 7).

---

## 2. A routing key in the binary publish frame (#549)

### The finding that started it

The ingest scenario published **unkeyed**, and an unkeyed record resolves to
shard 0. Every "12-shard" measurement ever taken with `felix-loadgen` had
therefore been exercising a single log. A 12-shard stream and a 1-shard stream
measured identically (920 against 923 MB/s) because both were the same log.

Adding keys to the generator exposed the real cost:

| | throughput | broker CPU |
|---|---|---|
| unkeyed, 1 shard | ~917 MB/s | 55%, us=20 |
| keyed, 12 shards | **645.8 MB/s** | 63%, us=28 |

Routing a record cost roughly **30% of throughput**, with user CPU up from 20
to 28. The cause was framing, not routing: the binary publish layouts were
fixed and had nowhere to put a key, so a keyed publish fell back to the JSON
encoding. Sharding was something you paid for rather than something you got.

### The wire format

`FLAG_BINARY_PUBLISH_KEYED` (`0x0040`) is a modifier on
`FLAG_BINARY_PUBLISH_BATCH` (`0x0001`), the shape `FLAG_BINARY_PUBLISH_ACKED`
already established. The body is prefixed with:

```
u16 key_len
u8[key_len] key
```

The ordering decision that mattered: when the acked bit is also set, the
correlation prefix comes **first** and the key prefix follows it. That keeps
`peek_acked_publish_prefix` reading `request_id` at offset 0 whether or not a
key follows. That is what lets the broker answer a malformed body with an
error the client can still correlate to its pending request.

An empty key is a key. It hashes to a shard like any other, and is deliberately
not the same as an unkeyed frame, which always resolves to shard 0.

### Negotiation, not versioning

A broker predating `0x0040` would read `key_len` as `tenant_len` and produce
garbage, so the client sends the keyed binary frame only to a broker that
advertised the bit and uses JSON otherwise. The fallback costs throughput, not
correctness. This follows the existing rule that unknown flag bits are
*rejected* rather than masked off: masking one means confidently misparsing the
body.

> `a_keyed_publish_negotiates_the_binary_frame`: the broker advertises the
> bit, so the sharding tests exercise the binary path rather than silently
> falling back to JSON.

### The result

Measured on the live cluster after deploying the fix, storage wiped between
runs:

| | before | after |
|---|---|---|
| unkeyed | 917 MB/s (us=20) | 868–905 MB/s |
| keyed | 645.8 MB/s (us=28) | **895.6 / 925.8 MB/s** (us=21) |

Keyed and unkeyed are now statistically indistinguishable. The CPU signature
confirms the mechanism: keyed user CPU fell from 28 to 21, so the JSON framing
cost is what disappeared.

One correction worth recording. On the first keyed run after the fix, keyed was
reported as "~5.6% slower" than unkeyed. With more samples that gap was inside
run-to-run spread: two runs of an *identical* configuration produced 895.6 and
925.8, about 3% apart. The honest statement is indistinguishable, not slightly
worse.

---

## 3. io_uring device flushes (#548)

### The premise that was wrong

The storage module documented this:

> An `fsync` genuinely blocks, for milliseconds on real hardware… because
> flushes are grouped, one blocking task serves many appends.

On NVMe that does not hold. A flush here is a few hundred microseconds, which
makes the `spawn_blocking` hand-off a meaningful fraction of the operation
rather than a rounding error.

### What it changes

Every other way of issuing an `fsync` from async Rust hands the call to a
thread and waits for it to come back. `IORING_OP_FSYNC` removes the hand-off
rather than shrinking it: the request goes into a ring, the kernel performs it,
a completion arrives, and no thread is parked.

A rejected alternative is instructive. Running the sync *inline* also removes
the hand-off, but it destroys the `await` yield point, and background rollover
and retention depend on that scheduling. The inline experiment was built and
measured precisely to establish this, then discarded.

### A bug worth recording

The first version of the ring loop drained completions only when a *new*
submission arrived, so an in-flight flush with no traffic behind it could wait
forever. The fix is to never block on `rx.recv()` while any request is
outstanding. This defect does not appear under load, only at the tail of a
run, when the traffic that would have rescued it has stopped.

### The measurement, and a retraction

Issue #547 originally claimed the hand-off cost **17×** the syscall. That was
wrong, and the follow-up experiment refuted it: an inline `fsync` moved the
operation from 583 µs to 530 µs, about 9%. The 17× came from comparing
Felix-under-load against `fio`-at-idle. It is retracted on #547 and #548.

The real figure, from repeated runs:

| | runs (MB/s) | mean |
|---|---|---|
| `spawn_blocking` | 923.7, 913.6, 914.2 | 917.2 |
| `io_uring` | 926.0, 943.3, 973.2, 984.1 | **956.7** |

**+4.3%**, with no overlap between the distributions. An early single run had
suggested +6.5%; repeats brought it down.

### Why default off

Linux only, default off. The blocking pool remains the fallback for macOS
development and for any Linux that cannot build a ring (an old kernel, or a
container that forbids the syscall). Durability must not depend on an
optimisation being available. Before flipping the default: a CI job running the
storage suite with the flag on, and crash-recovery tests against the ring.

---

## 4. The run matrix

| run | keys | conns/gen | sharding | io_uring | aggregate | broker CPU | generator CPU |
|---|---|---|---|---|---|---|---|
| A | 0 | 4 | hash | off | 904.8 MB/s | busy=54% us=19 sy=21 si=10 wa=4 | not sampled |
| B | 12 | 4 | hash | off | 854.4 MB/s | busy=63% us=21 sy=23 si=10 wa=9 | not sampled |
| C | 12 | 4 | hash | on | 894.3 MB/s | busy=69% us=24 sy=23 si=9 wa=13 | not sampled |
| D | 0 | 4 | hash | on | 868.6 MB/s | busy=59% us=21 sy=21 si=9 wa=8 | not sampled |
| E | 0 | 16† | hash | on | 880.5 MB/s | busy=60% us=22 sy=21 si=9 wa=8 | not sampled |
| F | 0 | 16† | rr† | on | 891.5 MB/s | busy=59% us=22 sy=22 si=7 wa=8 | not sampled |
| G | 12 | 4 | hash | on | 842.1 MB/s | busy=66%, 16 publish workers | 22–31% |
| H | 12 | 4 | hash | on | 895.6 MB/s | busy=69%, raised admission | 23–31% |
| I | 12 | 4 | hash | on | 925.8 MB/s | busy=68% us=20 sy=21 si=13 wa=13 | 22–31% |
| J | 12 | 4 | hash | on | **void**: 3 of 4 generators failed to connect | busy=68% | 69% (one generator) |
| K | 12 | 4 | **rr** (verified) | on | 912.6 MB/s | busy=66% us=21 sy=20 si=15 wa=11 | not sampled |

† **Runs E and F are void.** The generator ignored client environment config
(#553), so the connection-pool and sharding overrides never took effect. Both
actually ran the default 4 connections and `hash_stream`.

Run K is the re-test of what F claimed to measure, on a generator carrying the
#554 fix. The knob was verified rather than assumed: the binary reports
`sharding=RoundRobin` when the variable is set. Round-robin lands at 912.6,
inside the band.

Every valid run lands between **842 and 926 MB/s**. Shard count, connection
count, flush mechanism, worker count and admission budget all move it by less
than the run-to-run spread, which two identical configurations (H and I) put at
about 3%. The broker never exceeds 69% busy; the generators never exceed 31%.

### Run J, which failed usefully

At 32 publishers per generator, three of four generators died with
`establish QUIC connection: timed out`. Each publisher constructs its own
client with its own 4-connection pool, so 32 publishers is 128 connections per
generator and 512 fleet-wide, more than the broker would accept quickly
enough.

The one generator that did connect produced **594.7 MB/s alone**, at 69% of its
own CPU. A single generator running alone reaches nearly three times what it
gets when four share the broker. The generators are not the constraint; each is
being handed a quarter of a broker-limited aggregate.

---

## 5. The ceiling: hypotheses tested and killed

**The commit sequencer.** The prior session's findings concluded the ceiling
was "commit-sequencer-bound" and predicted durable throughput scales with
commit paths. `CommitSequencer` lives on `StreamState`, which is keyed per
(stream, shard), so twelve shards is twelve independent commit paths on one
broker. Throughput did not move. **That prior conclusion is wrong.**

**The publish worker pool.** The pool is process-wide by deliberate design, a
handle maps to exactly one worker, each worker awaits its claim serially, and
the default is 4. A plausible funnel. Raised 4 → 16: **842.1 MB/s**, slightly
*worse*. Killed.

**Admission windows.** 64 MB process-wide, 16 MB per connection, queue depth 64
per worker. A byte budget is a window, and throughput under a window is budget
÷ residence time, which would explain invariance to everything else. Raised
16×, 16× and 64×: **895.6 MB/s**, no change. Killed.

A falsification check sharpened this before the run: Little's Law says a 64 MB
window binding at 890 MB/s requires ~72 ms average residence, while measured
ack p50 on this hardware is ~200 µs, 300× apart. The run agreed with the
arithmetic.

**A single saturated core.** Aggregate CPU of 60% can hide one core pegged at
100%, and QUIC softirq is exactly the kind of work that concentrates. Per-core
sampling under load: **all eight cores at 67–71%**, softirq spread evenly at
9–17%. No hot core.

**This eliminates less than it appears to, and the doc originally overstated
it.** Tokio is a work-stealing runtime: a single continuously-runnable task
migrates between worker threads, so at 1 Hz it smears across all eight cores as
moderate even load: the exact 67–71% signature measured. Per-core sampling can
separate "one pegged CPU" from "work spread across cores"; it *cannot* separate
"work spread across cores" from "one saturated task being migrated." The
single-serialisation-point family is therefore **untested, not eliminated**.
`tokio-console` would settle it directly by showing one task's busy time.

**The generators.** Sampled at **22–31% busy**. A single generator running
alone reached 594.7 MB/s, and an earlier campaign recorded one generator
sustaining 950–1151 MB/s. Killed.

**The disk.** `wa` of 4–13%; the device separately measured at 1,346 MB/s with
`fdatasync` across 4 jobs and 1,718 MB/s without. Felix at ~900 is roughly 67%
of the fsync'd figure. Not saturated.

**In-flight ack waiters.** A count limit rather than a byte budget, so it would
bind independently of payload size. Inspected and found not applicable to this
workload at all: ingest publishes `AckMode::None` and requests no acks. Still
relevant for acked workloads.

**Round-robin publish sharding.** The stream hash pins a publisher to one of
its client's 8 workers (4 connections × 2 streams), leaving 7 workers and 3
connections idle. Round-robin rotates per batch across all 8, an 8× change in
per-publisher stream parallelism, and `select_worker` is called per batch so
the rotation is genuine. Run F appeared to test this and did not (#553). Re-run
on a fixed generator, with the effective configuration printed as proof:
**912.6 MB/s**. Killed.

**The client's connection funnel.** Wrong for this workload; see section 7.

### The apparent contradiction, resolved

Two generators had previously produced **1.63 GB/s** aggregate, and one
generator had been measured at 1.09 GB/s. Now four generators produce ~890 MB/s
total, ~220 each: aggregate halved while generator count doubled.

The resolution is in the prior campaign's own notes: *adding a second or third
loadgen does not go faster*. One generator hit 950–1151 MB/s by itself; four
today sum to ~890. The aggregate is unchanged, and each generator simply
receives a quarter of a broker-side ceiling.

The 1.63 GB/s figure is a different rig and does not contradict this: per
`docs-site`'s real-network page it is **two load generators against three
`D4as_v5` brokers**, a `D4` contributing 1,084 MB/s and a `D2` adding 549,
so ~544 MB/s per *broker*. (An earlier revision of this document read it as a
two-broker session at ~815 per broker. That was wrong twice over: generators
and brokers were conflated, and the two generators were different sizes, so
there is no meaningful per-generator average to quote either.)

There is no per-generator limit. There is one broker ceiling near 900 MB/s,
divided by however many generators are pointed at it.

### The leading live hypothesis: one socket, one endpoint driver

Everything eliminated above sits **downstream** of QUIC packet intake. What
sits upstream of all of it is the quinn endpoint. `QuicServer::bind` binds one
UDP socket and constructs one `Endpoint` from it, so 256 connections are 256
consumers behind a single feeder: the socket reads and datagram routing are
one task's work.

This fits every observation on record:

- **Invariant to shards, workers, admission, flush mechanism**, all downstream
  of intake.
- **Invariant to connection count**: connections share the socket.
- **Generators idle at 22–31%**, blocked on a broker that cannot drain faster.
- **Device at ~67%**, never asked for more.
- **No hot core**: per the work-stealing note above, a saturated task does not
  produce one.
- **Scales with brokers and nothing else**: each broker has its own socket.

It is also continuous with this repository's own prior finding.
`docs/perf-investigation-throughput.md` concluded that quinn driver re-poll
latency becomes the pipeline's clock; the transport module still says so in
prose. Same component, one level up. Note that the mitigation from that
investigation is **not active here**: the I/O runtime pool defaults to 2 on
macOS and **0 on Linux**, deliberately, because isolating drivers measured
worse on Linux at the time.

Cheap diagnostics, before any code:

- `netstat -su` / `/proc/net/snmp` for `RcvbufErrors` and `InErrors`. Non-zero
  means the socket reader is behind and QUIC is retransmitting, which caps
  throughput while leaving CPU moderate.
- `ss -uanm` for receive-queue depth on the listening socket under load.
- Whether GRO is active on the receive path. Without it, 900 MB/s at a
  1500-byte MTU is roughly 600K syscalls/s.

The structural test is `SO_REUSEPORT` with N sockets and N quinn endpoints,
letting the kernel hash flows across them. If throughput scales with endpoint
count, that is the ceiling.

This *was* run. See section 9.

---

## 6. What sharding actually costs

Sharding did not help. It was also not free. The broker's own flush counters,
for two runs moving identical bytes:

| | logs | fan-in | device flushes | mean flush | total flush time |
|---|---|---|---|---|---|
| unkeyed | 1 | 1.875 | 153,589 | 0.575 ms | 88.6 s |
| keyed | 12 | **1.124** | **325,016** | **1.021 ms** | **331.9 s** |

Splitting one log into twelve made the device work **3.7× harder** for the same
payload. Two effects compound.

**Group commit dilutes.** Group commit amortises one `fsync` across concurrent
writers on the same log. With one shard, all 64 publishers batch into one
commit stream; with twelve, each log sees roughly a twelfth of the concurrency,
fan-in falls toward 1, and the flush count doubles. Sharding works directly
against the amortisation that makes durability affordable.

**Each flush gets more expensive.** This dilution does *not* predict: dilution
implies more flushes at the same unit cost. Mean flush went from 0.575 ms to
1.021 ms, **1.78×**. Twelve logs on one ext4 filesystem turn a single
sequential append stream into twelve interleaved ones sharing a journal, across
a RAID0 stripe. At the time of measurement the volume held 441 segment files.

### The inference that matters

Throughput barely moved while device flush work varied by 3.7×. You can triple
the disk work and the number does not care, so **the flush path is not the
binding constraint**, which is also why `io_uring`, a flush-path optimisation,
is worth only ~4% here.

### The reframe

Sharding is a **horizontal** lever, not a vertical one. Its purpose is to
spread work across brokers: more NICs, more cores, more crypto capacity, more
independent devices. Twelve shards on one broker share one NIC, one CPU and one
filesystem, so there is nothing to win and a little to lose.

The docs presented sharding as a performance lever without saying which kind,
which is what invites this experiment. #552 carries that correction.

---

## 7. Roadblocks

### Bugs in the instrument

**The generator ignored client config entirely (#553).** `felix-loadgen` built
its client with `ClientConfig::optimized_defaults`, which never reads the
environment, so every client-side variable was silently inert.

This is a measurement bug, not a missing feature. Three runs set the connection
pool and sharding mode to test whether client connection fan-out was the
ceiling; all three measured the default configuration, and two were initially
reported as evidence *against* a hypothesis they had never tested. A knob that
appears set and is not is worse than one that does not exist. Fixed in PR #554,
with `the_client_config_reads_the_environment` as the regression test.

**The harness measured the broker and not the generators.** A run where the
generators were saturated and the broker was not would read as a broker result.
Six configurations produced the same ~880 MB/s before anyone sampled the
generators; when the sample was added they were 22–31% busy, which eliminated
an entire branch of the investigation in one run.

**The generator published unkeyed.** Every "multi-shard" number this tool ever
produced was single-shard. That invalidated several standing conclusions at
once: why 12-shard and 1-shard streams showed identical fan-in, why `io_uring`
barely moved the number (one log holds one flush in flight, so there was no
concurrency for a ring to exploit), and why a two-broker session saw every
publish forwarded to one owner.

### Wrong conclusions drawn, and corrected

**The stream-hash funnel.** `select_worker` hashes on
`(tenant, namespace, stream)` only, so every publisher writing to one stream
pins to a single worker (one QUIC stream on one connection), and this was
announced as the explanation for the per-generator ceiling. It is a real
inefficiency, but it is **not** this ceiling: the ingest scenario constructs a
separate client per publisher, so each client serves exactly one publisher and
the hash has nothing to funnel.

**"Keyed is ~5.6% slower."** Reported from a single pair of runs; later runs of
the identical configuration straddled the unkeyed band.

**The 17× hand-off claim.** Filed in #547, retracted after the inline
experiment refuted it.

**"+6.5% for io_uring."** Quoted from one run; repeats settled at +4.3%.

The pattern across all four: one run is not a measurement, and a number that
flatters a hypothesis deserves more scrutiny than one that does not.

### Operational friction

**`az vm run-command` dispatch latency** is why an earlier CPU sampler,
dispatched mid-run, so often landed after a case had finished and reported an
idle machine. Samplers are now armed *before* the load and difference
`/proc/stat` at 1 Hz.

**`felix-broker --help` is not a flag.** Running it on the broker VM starts a
second broker process. This had previously contaminated a multi-generator run;
it recurred this session, was caught, and the process was confirmed gone before
any measurement. The correct move is never to invoke the binary on a live host.

**A silent install failure.** One generator reported success but still carried
the old binary: a stale `/tmp` path owned by another user made `curl` fail with
exit 23 while the surrounding script reported success. Caught only because
every deployment was verified by checksum rather than by exit code. A
subsequent install then picked up a third unrelated binary from that same stale
path.

**A sampler that expired before the load.** The first per-core sample ran its
iterations during a gap between runs and reported all eight cores at 5% busy.
It had to be re-armed while a run was demonstrably in flight.

---

## 8. What is still open

One broker plateaus at ~900 MB/s with ~31% of its CPU idle evenly across all
eight cores, generators 70–78% idle, the device at roughly 67% of its measured
`fdatasync` capability, twelve independent commit paths available and unused,
and admission budgets raised 16–64× with no effect. Every structural
explanation offered so far has been tested and eliminated except one:
a single saturated task upstream of everything measured, at the quinn endpoint.
That is now the leading candidate and it is untested. The ceiling
reproduces across ten runs here and matches what the previous campaign
recorded (~950–977 MB/s) before any of this work.

Recommended next, in this order:

1. **Two brokers.** The cheapest run, and it answers whether the ceiling even
   matters before more days go into it: the reframe predicts throughput scales
   with brokers. It also directly tests the endpoint hypothesis, since each
   broker has its own socket.
2. **Test the endpoint hypothesis.** The socket diagnostics above cost nothing.
   The structural test is `SO_REUSEPORT` with N endpoints.
3. **`tokio-console`, not just a flamegraph.** A flamegraph shows a hot
   *function*; the open question is whether a single *task* is saturated, which
   per-core sampling provably cannot answer. Look for one task's busy time
   dominating rather than for a hot symbol.
4. **Hash the routing key, not the stream name,** where a key is present.
   Ordering on a sharded stream is per key, so hashing the stream name funnels
   traffic that is free to spread. Wrong as written, independent of this
   ceiling.
5. **Correct the prior findings.** The commit-sequencer conclusion should not
   outlive this session.
6. **Re-examine published numbers.** Any figure produced by `felix-loadgen`
   before #554 came from a generator that ignored its own configuration and
   published unkeyed. Aggregate broker numbers are probably sound; anything
   characterising sharding or client tuning is not.

Related: #539. Perf session results were gitignored, so published numbers had
no auditable evidence trail. Fixed: the ignore rule was narrowed to
`sessions/*.env`, and the session output behind these numbers is committed
under `scripts/perf/azure/sessions/`.

---

## 9. The two-broker session: the ceiling, measured

A second session (`v042-2broker`) was provisioned specifically to test the
endpoint hypothesis and the horizontal-lever reframe at the same time. Two
`L8as_v4` brokers, four `D4as_v5` generators, shard ownership balanced **24/25**
across the brokers, storage wiped between runs, same keyed 12-shard load.

### Throughput scales with brokers, 2.1x

| run | brokers | config | aggregate | broker CPU |
|---|---|---|---|---|
| A | 2 | default | **1896.0 MB/s** | b0 54%, b1 65% |
| B | 2 | `FELIX_IO_RUNTIME_THREADS=2` | 1341.1 MB/s | b0 44%, b1 32% |
| C | 2 | default (repeat of A) | **1852.4 MB/s** | b0 54%, b1 62% |

Against ~900 MB/s on one broker, two brokers give **2.1x**, essentially linear.
Per-generator throughput doubled from ~220 to ~470 MB/s, which is the third
independent confirmation that the generators were never the constraint. Each
broker independently reproduces the "ceiling with CPU to spare" signature.

**The reframe is now measured, not argued: throughput scales with brokers (2.1x)
and not with shards (1.0x).**

### The endpoint driver is ~88% of one core

Per-thread CPU on broker-0 under sustained load, default configuration:

```
tokio-rt-worker  48.0%   tokio-rt-worker  47.8%
tokio-rt-worker  47.9%   tokio-rt-worker  47.8%
tokio-rt-worker  47.9%   tokio-rt-worker  47.7%
tokio-rt-worker  47.8%   tokio-rt-worker  47.4%
iou-wrk / felix-uring-fsync / OpenTelemetry   ~14% combined
TOTAL 396.1% = 3.96 cores of 8
```

Eight workers, uniform to within 0.6%. That is the work-stealing signature and
it is exactly why per-thread sampling cannot answer the question, which is the
correction recorded in section 5.

The stacks can. `perf record -F 199 -g` on the broker process, collapsed:

| component | share of process CPU |
|---|---|
| **endpoint driver receive path** (`poll_recv` → `recvmmsg`) | **22.26%** |
| `sendmsg` | 9.45% |
| AES-GCM (`aes_gcm_*`) | 7.62% |
| `felix_storage` | 4.26% |
| `epoll_wait` | 3.80% |
| futex (wake + wait) | 3.28% |
| `broker::` | 3.31% |
| `Connection::process_payload` | 1.90% |

22.26% of 3.96 cores is **0.88 cores**, and the endpoint driver is a *single
task*, so one core is its hard ceiling. It is running at ~88% of what it can
ever use, while the machine as a whole sits at half idle.

That is the ceiling, and it explains every invariance in section 4: shards,
workers, admission budgets, connection count and flush mechanism are all
downstream of a feeder that is already nearly saturated.

![felix-broker flamegraph](assets/perf/broker-flamegraph-2broker.svg)

### Two traps in reading this profile

**The 1.77% that wasn't.** `poll_socket` resolves as a symbol in only 1.77% of
samples. Read alone, that number kills the hypothesis, and it was briefly
reported as doing so. It is an artifact of a release build without frame
pointers: most stacks reaching the syscall resolve as `[unknown]` above it. The
syscall itself (`recvmmsg`) is called from exactly one place in this process, so
it is the reliable proxy. **Symbol coverage, not CPU share, is what differs
between the two numbers.**

**`FELIX_IO_RUNTIME_THREADS` does not isolate what it appears to.** Run B pinned
the endpoint driver to a dedicated thread, which read `felix-quic-io-0` at
**99.9% of one core**, an apparently perfect confirmation. It is confounded:
`IoRuntime::spawn` places *every* quinn task on that runtime, so all 256
connection drivers and their AEAD land on the same thread. The flamegraph shows
`aes_gcm_dec_update` and `Connection::process_payload` on that thread, and
throughput drops 29% (1341 vs 1896). The 99.9% says a thread doing routing plus
all connection crypto saturates; it does not isolate the feeder. The default
configuration plus stack analysis is the measurement that does.

### What this means for the fix

The per-broker ceiling is one task on one core doing socket receive and datagram
routing. It is a scaling property of every Felix broker, not an artifact of this
rig, and no amount of tuning downstream of it will move the number.

The fix is more endpoints per broker: N sockets under `SO_REUSEPORT` with N
quinn endpoints, or the cheaper interim of N listeners on N ports advertised
through the control plane. The hazard to design for is that the kernel hashes
`SO_REUSEPORT` by 4-tuple while QUIC identifies connections by connection ID, so
a migrating client rehashes to a socket whose endpoint does not own it; the
production answer is an eBPF socket selector keyed on the CID, with the endpoint
index encoded in the connection ID quinn generates.

Until then, the honest deployment guidance is the one this session started by
disproving and ended by confirming: **scale Felix with brokers.**

## 10. A local attempt on macOS, and why it settles nothing

`crates/testing/felix-cluster/tests/listener_throughput.rs` (`#[ignore]`d) sweeps
`FELIX_QUIC_LISTENERS` on one broker, to see whether the multi-listener fix
(#558) actually moves the ceiling section 9 measured. Run on an M4 Max, 16
cores, with `FELIX_INITIAL_MTU=1350` (macOS loopback otherwise pins a
16336-byte MTU, ~11x a real path, which would understate the driver's
per-byte work), 12 shards and keyed batches.

Flat: 1.00x / 1.00x / 1.00x / 0.98x at 1, 2, 4 and 8 listeners. Not evidence
the fix does nothing: the rig never reached the regime section 9's endpoint
driver finding describes:

- **The broker was nowhere near driver-bound.** ~2.5 of 16 cores, ~13 idle.
  The ceiling this lifts is a single task pegged at one core; a broker at 2.5
  cores spread across many is not hitting it.
- **The generator was the limiter, on one thread of it.** `io_runtime_index`
  sends every *client* endpoint to `pool_len - 1`, so all 16 clients in the
  test process funnel their QUIC drivers onto one thread, measured at 99.7%
  while the next busiest client thread sat at 3.4%. Four publishers and 32
  publishers gave identical throughput because the measurement was one
  saturated client thread throughout. Section 9's session used four separate
  generator VMs for exactly this reason.
- **`AckMode::None` makes the client-side counter an enqueue rate.**
  `client-sent` read ~770 MiB/s while the broker's own
  `felix_storage_append_bytes_total` read ~543 MiB/s over the same window.
  Only the broker-side number is real.

Ruled out on the way: storage bandwidth (the temp device does 4.19 GB/s, 7x
the observed rate), broker admission (`FELIX_BROKER_PUBLISH_INFLIGHT_BYTES=1GiB`
changed nothing, 511 vs 520 MiB/s stored), fsync (`none` is worth ~25%, not
the wall).

A single-process generator on macOS cannot discriminate this question. Tracked
as its own gap in #597.

### The finding that outlasted the run: the pool defeats N listeners at N=2

For servers `io_runtime_index` is `sequence % (pool_len - 1)`, and the macOS
default pool is 2, so `sequence % 1`, which is always 0. Every listener a
broker binds landed on the same I/O thread, measured here as the broker's
79.5% thread while its others sat at ~35%: the pool defeated the fix before it
could help.

Linux defaults the pool to 0, so drivers go to the app runtime and tokio
spreads them. This does not touch the design in section 9. But any
deployment setting `FELIX_IO_RUNTIME_THREADS >= 1` with a pool of 2 gets one
driver thread however many ports it binds. Filed as #596; a future session
should measure `FELIX_IO_RUNTIME_THREADS` in `{0, N+1}` rather than
rediscover this as a null result.

One more thing worth carrying forward: broker-stored throughput drifted down
slightly as listeners rose (543 → 527 → 499 → 485 MiB/s). Inside run-to-run
spread on a rig this noisy to trust, but worth re-checking where the
measurement is trustworthy: N endpoints may have a small cost of their own.
