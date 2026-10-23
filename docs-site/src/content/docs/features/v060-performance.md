---
title: "v0.6.0 Performance"
description: "What one Felix broker does on a real Azure cluster at v0.6.0: the exact conditions, how to read the numbers, the results, a NATS JetStream comparison, and how the numbers were found."
---

<!-- DRAFT. Everything marked TODO(data) is filled from the finished campaign in
scripts/perf/azure/sessions/v060-w3-final-results/. Do not publish with any
TODO(data) left. -->

This page reports what a single Felix broker does on a real network at v0.6.0.
It states every condition so the numbers can be reproduced, explains the three
different things people mean by "latency", gives throughput and latency for each
publish shape, compares Felix with NATS JetStream under identical conditions,
and says what limits each shape. The last section tells the story of how an
apparent 40% regression turned out to be a change in the load shape plus one
real broker defect.

## Summary

On one 8 vCPU broker with local NVMe, over QUIC with TLS:

- **About 3.7 GB/s of payload without losing anything.** In-memory 4 KiB
  messages in batches of 64, publishers waiting for room when the broker is
  full (`FELIX_PUB_INGRESS_WAIT=1`). Nothing was shed. This is the number to
  plan with if you cannot lose messages.
- **About 4.2 GB/s at most once.** The same load on the shipped defaults, where
  a fire-and-forget publish that arrives at a full broker is dropped instead of
  waiting. 1.2 to 2.2% of what was sent was dropped. The publisher is not told;
  the broker counts it.
- **About 1.4 GB/s durable, fsynced before the ack.** Durable 4 KiB in batches of
  64, acked after an fsync. That is the disk: the same volume does 1414 MB/s in
  `fio` with an fsync after each write.
- **This build is as fast as or faster than v0.6.0-preview.9** on every shape
  in the gate: up to 32% faster for small acked messages, unchanged for durable.

<!-- TODO(data): add the one-in-flight ack and delivery latency (lat cells) and
the NATS JetStream ratios when those steps finish. -->

## Exact conditions

Everything below was recorded by the campaign driver and is in the repository
under `scripts/perf/azure/sessions/v060-w3-final-results/`.

### Hardware and network

| Part | Setting |
|---|---|
| Region | Azure westus3, one resource group, one virtual network |
| Broker | 1 x `Standard_L8as_v4`, 8 vCPU, 64 GiB, kernel 7.0.0-1017-azure |
| Broker storage | The VM's 4 local NVMe drives as one md RAID0, ext4 mounted `noatime`, 1.7 TiB |
| Load generators | 4 x `Standard_D4as_v5`, 4 vCPU, 16 GiB |
| Control plane | 1 x `Standard_D2as_v5` |
| NIC | `hv_netvsc` driver, MTU 3900 on every VM |
| Transport | QUIC over UDP with TLS, as in production. Every byte is encrypted. |
| UDP buffers | `net.core.rmem_max` and `wmem_max` 26214400, `rmem_default` and `wmem_default` 8388608 |

The device baseline was taken with `fio` on the broker's data volume before the
campaign. Sequential 256 KiB writes with `fdatasync` ran at 1414 MB/s with one
job (p50 sync latency 71 microseconds, p99 889 microseconds), 1467 MB/s with
four and 1456 MB/s with eight. Sequential 4 KiB writes ran at 79.7 MB/s (19462
IOPS) with one job. These are the ceilings the durable cells are judged against.

<!-- TODO(data): CPU model string from `lscpu` if the facts files do not have it;
the recorded facts do not include one. -->

The preflight refused to start until every VM reported MTU 3900 with
unattended-upgrades masked, each load generator could reach the broker with full
size packets, the broker's data volume was the RAID0 device, and `fio` read at
least the expected rate. All checks passed (`final/preflight.txt`).

### Build

The broker and the load generators run commit
`88a9168148f6fab3245712ea8dee3dc110c9f778` of main, built with the release
profile and no extra cargo features. That commit contains four changes that
came out of the investigation in the last section:

- #1414: a publisher waiting for ingress room is woken when a slot frees,
  one per slot, instead of all of them at once.
- #1415: the default ingress queue depth is 512 per executor (2048 jobs with
  four executors) instead of 64.
- #1418: the client opens 4 shared publish streams for a keyed stream by
  default, not 16.
- #1419: hot-path metric handles are cached instead of registered on every
  publish.

The campaign also built v0.6.0-preview.9 (`71960125bd9a9dee61115a7553f21342a1d136d1`)
on the same VMs and ran it against this build as a gate on the headline cells.
A cell more than 3% slower than preview.9 would have stopped the campaign.

### Settings that differ from the product defaults

Most of the broker runs at its defaults. The main campaign sets the following
explicitly. Three differ from what you get if you set nothing.

| Variable | Measured value | Product default | Differs |
|---|---|---|---|
| `FELIX_ACK_ON_COMMIT` | `1` | `1` from v0.6.0 for durable streams (was `0`) | No, since #1421 and #1422 |
| `FELIX_PUB_INGRESS_WAIT` | `1` | unset (fire-and-forget is shed when ingress is full) | Yes |
| `FELIX_STORAGE_IO_URING` | `1` | `0` | Yes |
| `FELIX_DURABLE_FSYNC_MODE` | `on_commit` (`periodic` in the periodic latency cells) | `periodic` | Yes |
| `FELIX_QUIC_LISTENERS` | `4` | derived from the core count, which is 4 on 8 cores | No |
| `FELIX_IO_RUNTIME_THREADS` | `0` | see the environment reference | Harness setting |
| `FELIX_UDP_RECV_BUFFER`, `FELIX_UDP_SEND_BUFFER` | `25165824` (24 MiB) on broker and clients | OS default | Yes |
| `FELIX_MTU_UPPER_BOUND` | `3872` on broker and clients (NIC MTU is 3900) | built-in bound | Yes |

<!-- TODO(data): confirm the FELIX_ACK_ON_COMMIT default row against the merged
#1422 and the FELIX_IO_RUNTIME_THREADS and MTU bound defaults against
docs-site/src/content/docs/reference/environment-variables.md. -->

The clients set only the two UDP buffer sizes and the MTU bound. They use no
connection pool override, no ack threshold override and no shard stream override,
so `FELIX_PUB_SHARD_STREAMS` is the default of 4.

What the two durability settings promise:

- `FELIX_DURABLE_FSYNC_MODE=on_commit`: the broker fsyncs before it answers an
  acked publish, and many waiting publishes share one fsync (group commit). A
  crash, including a power loss, loses nothing that was acknowledged.
- `FELIX_DURABLE_FSYNC_MODE=periodic` (the default): a background timer fsyncs
  every 250 ms (`FELIX_DURABLE_FSYNC_INTERVAL_MS`). A broker process crash loses
  nothing that reached the page cache, but a power loss or kernel panic can
  lose up to one interval of acknowledged records.
- `FELIX_ACK_ON_COMMIT=1`: an acked publish on a durable stream is answered
  after the write rather than when the broker accepts it. Until v0.6.0 the
  default was ack on accept. With `on_commit` fsync the answer means "on disk".
  With `periodic` fsync it means "written, fsync within one interval".
- `FELIX_PUB_INGRESS_WAIT=1`: a fire-and-forget publish waits for ingress room
  instead of being shed when the queue or byte budget is full. That is why the
  fire-and-forget numbers below are what the broker sustained, not what was
  offered. With the default, overload is shed and counted.

A separate set of cells runs with ack on accept (`FELIX_ACK_ON_COMMIT=0`, the
default of the measured build; v0.6.0 changes it), ingress wait unset and
io_uring off, to show what the settings cost. `on_commit` stays set in those
cells so they are comparable with the durable rows above.

### Workload

- Keys are spread evenly over all 48 shards of the stream. The load generator
  asks the broker for the shard count and names keys to hit each shard in turn.
  Some cells use plain keys (`k0` to `k47`) to show the effect of this choice.
- 4 load generators, 16 publishers each, 64 acked publishes in flight per
  publisher unless a cell says otherwise.
- Payloads of 256 B, 4 KiB and 64 KiB; batch 1 and batch 64; acked and
  fire-and-forget.
- Each cell runs 90 seconds. The generators start at a shared wall-clock time.

### How a number is taken

Throughput is measured at the broker, never at the clients. The broker records
its counters once a second. A cell's number is the median one-second rate over
the window in which all four generators were running, less its first and last
10%. Per-generator rates are never summed. The generators start and finish at
different times and get unequal shares, so summing their averages overstates or
understates the true rate by a wide margin. For fire-and-forget the client-side
figure is an enqueue rate and is not used.

Three rates come out of those counters:

- **Payload**: the bytes of message payload the broker accepted
  (`felix_publish_bytes_total`). A publish the broker sheds is not counted. This
  is the headline for in-memory cells.
- **Append**: the bytes written to the durable log, payload plus the record
  framing. This is the headline for durable cells, because it is what the disk
  has to absorb.
- **Wire**: the UDP bytes received on the broker's QUIC ports, with QUIC, TLS
  and Felix framing. It is 2 to 3% above payload for 4 KiB batches and about 25%
  above it for single 256 B messages.

Shed is what the load generators sent minus what the broker accepted, from
their own byte counts at the end of the cell.

Headline cells run three trials and the rest run two, interleaved so drift in the
VMs, the disk or the network hits both arms equally. Where trials are shown the
median is used and the spread is reported. Durable cells start from a wiped data
directory and dropped page cache.

### Reproduce it

The driver is committed. It starts the session, runs the preflight, records the
system facts, runs the gate, the matrix, the latency cells, the NATS pairings and
the sweeps, then deallocates the VMs:

```bash
cd scripts/perf/azure
SESSION=<name> BUILD_SHA=88a9168148f6fab3245712ea8dee3dc110c9f778 ./campaign-final.sh --dry-run
SESSION=<name> BUILD_SHA=88a9168148f6fab3245712ea8dee3dc110c9f778 nohup ./campaign-final.sh &
```

The dry run lists every cell, the expected hours and the cost. The README in that
directory describes the cell format and `summarize.py`, which turns a results
directory into `cells.csv` and `summary.md`. This run was trimmed to fit its time
limit: 64 KiB batch-1 cells, plain-key batch-64 cells, listener counts 1 and 8,
the 64 MiB window arm, the 4-in-flight arm of the latency curve and the second
trial of the sweeps were dropped.

`scripts/perf/azure/v060_tables.py <results-dir>` prints every table on this
page from the cell directories: median of the finished trials, the min to max
spread, and how many trials finished.

## How to read the numbers

"Latency" names three different measurements. Mixing them up is the most common
way to misread a benchmark.

**Unloaded latency.** One publisher, one publish in flight: send, wait for the
ack, repeat. This is the time a request takes when nothing else is happening, and
it is where the sub-millisecond figures come from. It is the number to use when
you ask how long one publish takes.

**Latency under offered load.** The same measurement with more publishes in
flight at once, 1, 16 and 64 per publisher. Latency rises as the in-flight count
rises, because publishes now wait behind each other. The curve shows where the
broker stops getting faster and starts queueing.

**Latency at saturation.** When the offered load is at or above what the broker
can do, the load generators keep a fixed number of publishes outstanding and
every one of them waits in a queue. The latency in that regime is queueing time,
and Little's law says what it must be: the number in flight divided by the
throughput. With 64 publishers each holding 64 in flight, roughly 1 GiB of
payload is in flight at 4 KiB. At about 4.7 GB/s that is 1 GiB / 4.7 GB/s, or
about 211 ms at p50. Nothing is slow in that cell; the broker is moving 4.7 GB/s
and each message waits its turn. The same broker answers a lone publish in under
a millisecond.

Saturation latency is reported on this page only to show queueing and the tail.
It is never the request latency of the system.

## Results

Each number is the median of the finished trials. Where trials differ by more
than half a percent, the range follows in parentheses.

### This build against v0.6.0-preview.9

Before measuring anything else, the campaign ran both builds on the same VMs,
two trials each, alternating, with identical clients and settings. The new build
had to be within 3% of preview.9 on every shape or the campaign would stop.

| Shape | preview.9 MB/s | This build MB/s | Change |
|---|---|---|---|
| In-memory 256 B, batch 1, acked, keys spread over all shards | 96 | 104 | +8.0% |
| In-memory 256 B, batch 1, acked, plain keys | 70 | 93 | +32.0% |
| In-memory 256 B, batch 64, acked, keys spread | 2168 | 2268 | +4.6% |
| In-memory 256 B, batch 64, acked, plain keys | 2016 | 2215 | +9.9% |
| In-memory 4 KiB, batch 64, fire-and-forget | 3597 | 3657 | +1.7% |
| Durable 4 KiB, batch 64, acked after fsync | 1410 | 1414 | +0.3% |

In-memory rows are payload, the durable row is append. The gains come from the
four fixes listed under [Build](#build). Durable throughput is set by the disk
and did not move.

### Throughput by shape

<!-- TODO(data): in-memory and durable (on_commit) x 256 B / 4 KiB / 64 KiB x
batch 1 / 64 x acked / fire-and-forget. Columns: broker append MB/s, messages/s,
broker cores used (vmstat or the broker's cpu series), fraction of the fio
baseline for durable cells. Plain-key rows for in-memory 256 B batch 1 and
batch 64 acked. -->

### Latency

<!-- TODO(data): one-in-flight ack and delivery latency p50/p99/p999 for
on_commit, periodic and in-memory, 256 B and 4 KiB, 3 trials. State the fsync
mode in every row. -->

### Latency against offered load

<!-- TODO(data): in-flight 1 / 4 / 16 / 64 per publisher for in-memory and
on_commit, 256 B and 4 KiB, acked batch 1: p50, p99, throughput. 4 is present
only if the trim kept it. -->

### Shipped defaults against the durability profile

The main campaign runs what we call the durability profile: acks after the
write, publishers wait for room, io_uring on. These cells run the same loads with
the measured build's defaults instead: ack on accept, no waiting, io_uring off.
Two trials each.

| Load | Durability profile | Defaults |
|---|---|---|
| In-memory 4 KiB, batch 64, fire-and-forget | 3657 MB/s payload, nothing shed | 4169 (4133-4205) MB/s payload, 1.2 to 2.2% shed |
| Durable 4 KiB, batch 64, acked | 1414 MB/s append | no result: publishes refused (below) |
| Durable 256 B, batch 1, acked | <!-- TODO(data): m2 cell --> pending | 74 (73-74) MB/s append, ack p50 4.1 ms, p99 99 ms |
| Durable 256 B, one publish in flight | <!-- TODO(data): lat cells --> pending | ack p50/p99/p999 160/200/217 us, delivery 294/376/419 us |

**Fire-and-forget under overload.** With the defaults, the broker drops a
fire-and-forget publish that arrives while its ingress queue or byte budget is
full, counts it in `felix_broker_ingress_dropped_total`, and keeps reading. The
publisher is not told. Delivery is at most once. The broker then accepts about
14% more payload than when publishers wait, at the cost of 1 to 2% of messages.
With `FELIX_PUB_INGRESS_WAIT=1` the publisher waits for room instead and
nothing is lost. Pick the second if a lost message matters to you.

**Acked publishes under overload.** With the defaults, an acked publish that
finds the byte budget full is refused with an error: "publish ingress byte
budget exhausted". The application gets the error and decides whether to send
again. Felix does not retry it for you. The load generator treats any refused
publish as fatal, so both trials of the durable 4 KiB cell stopped within
seconds, as soon as the disk fell behind. With `FELIX_PUB_INGRESS_WAIT=1` the
same load is paced to the disk and runs at 1.41 GB/s. If you publish durable
data faster than your disk writes it, either turn on ingress wait or handle the
refusal.

**What an ack means in these cells.** With ack on accept, the 160 us ack says
the broker has the message, not that it is on disk. The delivery time of 294 us
is later in the path: the broker hands a durable record to subscribers only
once it is durable, which with `on_commit` means fsynced. v0.6.0 acks durable publishes after the write by
default (#1421).

### The earlier 4.1 GB/s figure

The site previously quoted 4.1 GB/s for in-memory 4 KiB fire-and-forget. To
check it, the campaign ran the load generator that produced that figure next to
the current one, on this build, with publishers waiting for room:

| Load generator | Trial 1 | Trial 2 |
|---|---|---|
| Current | 3620 MB/s, 744 UDP drops/s | 3619 MB/s, 957 UDP drops/s |
| The one behind the 4.1 GB/s figure | 3556 MB/s, 1111 UDP drops/s | 4081 MB/s, 108 UDP drops/s |

The older generator reached about 4.1 GB/s once and about 3.6 GB/s once. The run
that reached it is also the one where the broker's socket buffers almost never
overflowed. The default-settings cells above, at 4.2 GB/s, show the same pattern:
25 drops a second. So 4.1 GB/s is real, but it does not reproduce every time
under the conditions on this page, and we report the number that does.

<!-- TODO(data): reading of the drop pattern once the listener and window sweeps
finish. -->

### Listener sweep

<!-- TODO(data): FELIX_QUIC_LISTENERS 1 / 2 / 4 / 8 on in-memory 4 KiB b64
fire-and-forget. 8 listeners also set FELIX_PUB_CONN_POOL=8. Only the listener
counts kept by the trim. -->

### Receive windows

<!-- TODO(data): default against 32 MiB (and 64 MiB if kept)
FELIX_BROKER_PUB_CONN_RECV_WINDOW / FELIX_BROKER_PUB_STREAM_RECV_WINDOW on the
same cell. -->

### UDP drops

The broker's kernel counts datagrams it dropped because a socket receive buffer
was full (`udp_rcvbuf_errors`). QUIC retransmits a dropped datagram, so a drop
costs a little bandwidth and some latency but no records are lost. The earlier
comparison runs showed this counter near zero for 256 B and in the hundreds to
about a thousand per second for 4 KiB batches (18 to 43 per second and 858 to
1035 per second in the comparison cells), so it is worth reading next to a 4 KiB
result rather than ignoring.

<!-- TODO(data): drops per second for every in-memory and durable cell, taken from
the broker's before/after netstat counters in each cell, with the one-sentence
reading per row. The earlier comparison reported 0.07% at 4 KiB: confirm the
final percentage as drops over inbound datagrams. -->

## Felix against NATS JetStream

<!-- TODO(data): pairs from nats/interleave.sh and nats/nats-latency.sh:
throughput ratio per pair with server CPU, latency pairs (256 B and 4 KiB,
on_commit/periodic/in-memory/core), and a sentence for every pair NATS wins or
ties. -->

The comparison follows these rules, because a benchmark that favors one side is
not worth publishing:

- NATS runs at its best, not its defaults. Its stream count was tuned first
  (`STREAM_TUNE`, 48, 96 and 192 streams) and the best value was used, because
  JetStream spreads work by stream the way Felix spreads it by shard.
- Same VMs. NATS server 2.15.0 and natscli 0.5.0 replaced the Felix broker on
  the same L8as_v4 and the same four D4as_v5 generators, with TLS.
- Cells are interleaved, Felix then NATS then Felix, so drift hits both.
- Every durability difference is stated next to the number. JetStream's fsync
  behavior and Felix's `on_commit` and `periodic` modes do not match exactly, and
  the pairs are labeled with what each side promises.
- Where NATS is faster, the page says so.

### How each pair is matched, and where it still differs

The two systems are not built the same way, so no pair is a perfect copy of the
other. Each pair is matched on what the publisher is promised when it gets its
answer, as closely as the two systems allow. The table says what each side
does, and the last column says what still differs. Read every ratio together
with its row.

| Pair | Felix | NATS JetStream | What still differs |
|---|---|---|---|
| Durable, one at a time | Durable stream, batch 1, 64 in flight. The ack comes after the record is fsynced (`on_commit`). | File stream with `sync: always`, fast-batch publish with flow 1. The ack comes after the write is fsynced. | Both fsync before the ack. Felix group-commits the writes that are waiting together into one fsync. JetStream fsyncs per write. |
| Durable, 64 per request | Batch of 64 records in one request, 64 requests in flight, fsynced before the ack. | File stream, `sync: always`, fast-batch publish with flow 64. | Felix sends 64 records as one request. NATS fast batch acks once per flow of 64 messages. |
| Durable, atomic 64 | Batch of 64 records, fsynced before the ack. | File stream, `sync: always`, atomic batch publish of 64. | NATS's atomic batch is all-or-nothing. <!-- TODO(verify): state whether a Felix batch is all-or-nothing on failure, with the code reference. --> |
| Periodic fsync, 64 per request | Durable stream with `periodic` fsync. The ack comes before the fsync. | File stream with the default sync. The ack comes before the fsync. | Both can lose the last interval's writes on a power loss. The intervals differ. <!-- TODO(verify): Felix periodic interval and JetStream's default sync_interval, with sources. --> |
| In memory | In-memory stream, acked. | Memory-storage stream, acked. | Neither touches disk. Both keep the records and can replay them. |
| Fire and forget | Unacked publish into an in-memory stream. Every record is appended and can be replayed by offset. | Core NATS publish to a subject. The server routes the message and stores nothing. | **Not the same work.** NATS forwards and forgets. Felix appends to a log. Use this pair to compare fire-and-forget rate, not storage. |

Two differences apply to every pair:

- **Transport and client.** Felix uses QUIC with TLS and its Rust load
  generator. NATS uses TCP with TLS and `nats bench` (Go). Both are each
  system's own tool, run with the same publisher count and payload.
- **What is counted.** Felix rates come from the broker's own counters (bytes
  appended, or accepted for in-memory and fire-and-forget). NATS rates come from
  the server's counters (records appended to the stream, or publish requests for
  core). Both count payload at the server, not at the client.

## What limits each shape

These are the limits we measured or can explain. They tell you what to expect on
hardware that is not this VM.

**Small messages are CPU-bound per message.** At 256 B the broker spends its
eight cores on per-message work: parsing, scheduling, ack writes and QUIC packet
handling. Throughput in MB/s is low, in messages per second it is high, and it
scales with cores and with how well the client batches. Batch 64 divides the
per-message cost over many records. Expect throughput to follow single-core
speed and core count.

**Large in-memory messages are bound by the CPU spent on bytes.** At 4 KiB batch
64 the cost is encryption, copying and UDP receive. Adding listeners spreads UDP
receive across cores. Expect it to follow core count and NIC bandwidth.

**Large durable messages are bound by the disk.** Durable 4 KiB batch 64 acked
runs close to the device's sequential write rate with fsync, which here is 1414
MB/s from `fio`. On a slower disk, durable throughput follows the disk with
group commit keeping fsyncs few. A cloud disk with a lower write cap lowers it
directly. Durable small messages are limited by commit latency per publish, and
more publishes in flight per commit raise throughput until CPU takes over.

**The NIC and the path.** A larger MTU means fewer packets per byte. The
campaign used MTU 3900 because the Azure virtual network accepts it. At the
default 1500 the same load needs more than twice the packets.

**Per-shard publish streams are a trade-off.** A client can give each shard of
a keyed stream its own QUIC publish stream so that a stalled shard (a quorum
wait, a dead follower) does not hold up healthy shards on the same connection.
Streams cost broker work. In-memory 256 B batch 1 acked with keys over 48
shards on an 8 vCPU broker:

| `FELIX_PUB_SHARD_STREAMS` | MB/s |
|---|---|
| 0 (pooled streams) | 162 |
| 4 | 125 |
| 16 | 97 |

Up to that many streams are opened per connection, assigned first come, first
served by shard on that connection. A client with 4 streams and 48 shards puts
the first four shards it publishes to on their own streams; later shards share.
A stalled shard then holds up only the shards that share its stream. The default
of 4 is the compromise. Set the variable to the stream's shard count for full
isolation, or 0 for the most throughput. #1417 tracks opening dedicated streams
only for shards that stall.

<!-- TODO(data): the numbers above are 161.6, 126.7/124.6 and 97.1/97.4 from
#1417 (measured at queue depth 1024 before the final build). Re-measure 0/4/16 on
the final build if the campaign time allows, and state the build. -->

**Core shards are neutral.** Once the wake-up problem described below was fixed,
the number of broker core shards no longer changed throughput in these cells.

## How we got here

A performance page that only reports the good outcome hides what the numbers
depend on. This is what happened between the first campaign and this one.

**The first numbers.** An earlier campaign measured in-memory 256 B batch 1
acked at 135.8 MB/s on a single 8 vCPU broker, with the broker CPU-bound at
7.35 cores. The same cell on v0.6.0-preview.9 measured 87.0 MB/s at 7.36 cores,
about 40% lower and about 55% more broker CPU per message. Batched cells fell
too: 256 B batch 64 acked from 2521 to 1905 MB/s.

**Wrong turns on the way.** The first suspect was the per-shard publish streams
that landed in between. They quadruple the number of inbound datagrams, so the
theory was reasonable, but changing them moved throughput by under 3% in the
comparison cells. A second theory held that the broker's cost is per inbound
datagram and the client was sending thinner packets. Three client changes built
on it (a frame-draining writer, a concurrent ack reader and a linger) all
measured worse on Azure and were dropped. Single-trial noise in these cells was
larger than assumed: one build measured between 66 and 94 MB/s across runs.
Durable cells also looked 35 to 38% lower for a day. That was the disk, not
Felix. The VM had been deallocated and came back with a local NVMe that ran
at 18% of its spec in eastus2. A `fio` baseline on the data volume showed it,
and the campaign moved to westus3, where the device matched the original
baseline (1416 against 1427 MB/s). On that disk durable cells were not regressed:
256 B batch 1 went from 27.1 to 34.4 MB/s and 4 KiB batch 1 from 268 to 354
MB/s.

**The load shape.** A bisect over the load generator's own history, with the
broker fixed, landed on the commit where the generator started learning the
shard count from the broker and spreading key names evenly over every shard
(#924). Nothing else in the client changed. On one binary, in-memory 256 B
acked: batch 1 ran at 108 MB/s with the old plain keys and about 62 to 64 MB/s
with spread keys; batch 64 ran at 2208 to 2320 against 1672 to 1710 MB/s. The
original campaign's key pattern was lumpy, and lumpy load lets a backlog build
up that makes each wake-up and lock acquisition serve several messages. Evenly
spread load pays for them one message at a time. The 40% was real for spread
keys, and it was a property of the broker, not of the measurement.

**The broker defects.** Profiling the same broker with plain and spread keys
showed where the extra cost went. A publish woke an executor every time, locked
the partition twice, and registered a metric through a hash lookup on every
publish. Separately, the default ingress queue held 256 jobs while about 4096
publishes were in flight. When it filled, every blocked publisher was woken
whenever any slot freed, so one freed slot woke thousands of submitters that
mostly went back to sleep. That is the thundering herd pattern we had already
fixed once for a different queue (#511). The fix admits one waiting publisher per
freed slot, oldest first, with the fair queue still deciding who goes next
(#1414). A full queue also stops the connection's control loop from reading its
stream, which stalls every stream on that connection; #1416 tracks making
backpressure per stream.

**Queue capacity.** Raising the default depth to 512 per executor (#1415) gave
the full gain: 256 B batch 1 acked went from 95 MB/s at depth 64 to 163 MB/s at
512, with p50 dropping from 12.6 ms to about 7.5 ms, and 1024 gave the same
throughput. Throughput at 4 KiB batch 64 stayed flat at 4.69 to 4.72 GB/s at
every depth, while the 4 KiB p99 fell from 458 to 597 ms at depth 64 to 358 to
384 ms at 512. We did not derive the capacity from the byte budget. A queued
job costs 2 KB or more with spread keys against a 512 B charge, the per-tenant
fairness reserve cannot be reached under a byte-derived capacity, and overload
becomes invisible because shedding starts about 500 times later.

**Shard streams.** With the queue fixed, the remaining gap was the shard stream
cost in the table above, so the default dropped from 16 to 4 (#1418).

**Lessons for measuring.**

- Baseline the hardware every session. A disk at 18% of its spec looked like a
  Felix regression for a day.
- A benchmark's load shape is part of the result. Changing how keys are spread
  moved throughput 40%, more than any code change in the range.
- Compare against a recorded configuration, not a remembered number, and rerun
  the old build on the new VMs.
- Run each cell more than once. One cell moved 66 to 94 MB/s between runs of the
  same build.
- A theory that fits the data can still be wrong. Measure the change before
  building on the theory.
- Report saturation latency as queueing time, and unloaded latency as latency.

## Charts

<!-- TODO(data): generate from the raw cell data by a committed script and
embed under /charts/perf-v060/. Planned charts:
  1. throughput by shape (in-memory and durable, grouped by payload and batch)
  2. durable throughput as a share of the fio baseline
  3. latency against offered load (in flight 1/4/16/64), p50 and p99
  4. one-in-flight latency by durability mode
  5. shipped defaults against the durability profile
  6. listener sweep
  7. Felix against NATS, ratio per pair
  8. shard streams 0/4/16
Script: scripts/perf/v060_charts.py was added by the old draft (#925) and is not
on main. It reads the old campaign's data.csv and the old chart set. Rewrite it
to read the cells.csv written by summarize.py for this campaign, drop the charts
of the old data (cost model, experiments, per-record, latency-927) and add the
list above. Keep the matplotlib requirement in scripts/perf/requirements.txt. -->
