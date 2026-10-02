---
title: "v0.6.0 Performance Review"
description: "Where Felix's throughput ceilings are on Azure, what sets them, what we changed to raise them, and how the numbers were measured."
---

:::caution[Draft]
This page is a draft for review. The sections on method, ceilings, experiments
and harness lessons use final data. Sections marked TODO(data) wait on the
final confirmation run on main and on runs still in progress.
:::

This review covers the v0.6.0 performance campaign on Azure. It says where
Felix's throughput stops, what stops it, which changes moved the limit and
which did not. Every number names the cell it came from. Cells live under
`scripts/perf/azure/sessions/<session>-results/cells/`, and the charts are
generated from them by `scripts/perf/v060_charts.py`.

## Headline numbers

:::caution[TODO(data)]
TODO(data): headline numbers from the final confirmation run of sessions A, B
and C on main with every merged change. Until that run, the best figures on
this page come from build `801fc22d` with the settings listed under
[Where the ceilings are](#where-the-ceilings-are).
:::

## Test setup and method

### Machines

Three sessions ran, each in its own region and resource group.

| | Session A | Session B | Session C3 |
|---|---|---|---|
| Region | eastus2 | westus3 | centralus, zones 1/2/3 |
| Brokers | 1 × `Standard_L8as_v4` (8 vCPU, 64 GiB) | 3 × `Standard_D4as_v5`, RF=1 | 3 × `Standard_D4as_v5`, RF=3 |
| Broker data disk | 4 local NVMe drives, RAID0 (`md0`), ext4 | 128 GiB Premium SSD (P10), ext4 | 128 GiB Premium SSD (P10), ext4 |
| Generators | 4 × `Standard_D4as_v5` | 2 × `Standard_D4as_v5` | 2 × `Standard_D4as_v5` |
| Control plane | `Standard_D2as_v5` | `Standard_D2s_v5` | `Standard_D2s_v5` |
| Broker builds | `167120f0`, `801fc22d`, `1db235da` (mimalloc) | `167120f0` | `167120f0` |
| Results | `v060-a2-results` | `v060-b2-results` | `v060-c3-results` |

Every VM ran kernel `6.17.0-1022-azure` with accelerated networking. On
session A the NIC is a Mellanox `mlx5_core` virtual function behind
`hv_netvsc` on the broker and on all four generators
(`sessions/logs/experiments-a.log`). The NICs start at MTU 1500. From the MTU
experiment onward, session A's broker and generators ran at MTU 3900, with a
`ping -M do` path check from each generator
(`system/nats/*.mtucheck.txt`). Sessions B and C stayed at 1500.

`167120f0` is main before the per-record copy fix. `801fc22d` is the merge of
#905 and also contains #901 and #903. The profile cells ran frame-pointer
builds of the same commits (`main-fp`, `801fc22d-fp`). Sessions A and B used
the load generator from `perf-loadgen-window@16321bec`; C3 used
`main@13b6003e`.

Session A's disk baseline, from `fio` with 256 KiB sequential writes and
`fdatasync` after each (`v060-a2-results/system/felixperf-broker-0.fio.txt`):

| fio test | MB/s | sync p50 | sync p99 |
|---|---|---|---|
| 256 KiB, 1 job | 1426.8 | 71 µs | 2.6 ms |
| 256 KiB, 4 jobs | 1483.1 | 97 µs | 5.5 ms |
| 256 KiB, 8 jobs | 1477.0 | 235 µs | 9.2 ms |
| 4 KiB, 1 job | 51.3 | 95 µs | 117 µs |

:::caution[TODO(data)]
TODO(data): a fio baseline on session B's and C3's P10 data disks. The fio
file in `v060-b2-results/system/` was recorded before the data disks were
attached and measured the OS disk, so it is not quoted here. See
[Harness lessons](#harness-lessons).
:::

### Workload

Unless a section says otherwise, session A's throughput cells use this
workload. Each of the four generators runs 16 publishers that send 4 KiB
records in batches of 64, keyed over 12 keys, for 90 seconds. Publishes are
fire-and-forget, and the broker runs with `FELIX_PUB_INGRESS_WAIT=1`, so a full
ingress budget slows the publisher down instead of dropping. Broker and
generators use 24 MiB UDP socket buffers. The broker runs with
`FELIX_STORAGE_IO_URING=1` and `FELIX_ACK_ON_COMMIT=1`, so a durable ack always
means the record is on disk. The in-memory cells write to stream `perf`, the
durable ones to `perf-durable` with `on_commit` fsync.

### How steady state is measured

All generators get the same start time (`--start-at`) and the same duration,
so they overlap for almost the whole run. The headline figure for a cell
comes from the broker, not the clients. The broker's agent samples its
counters once a second: appended bytes, publish bytes, bytes received on each
client port, UDP datagrams and drops, and the broker process's CPU ticks.
`scripts/perf/azure/summarize.py` then does the following:

1. It takes the window when every generator was running: from the last
   generator's start to the first generator's end.
2. It drops the first and last 10% of that window.
3. It cuts the rest into one-second steps, sums each step over brokers, and
   reports the median step.

Most cells on this page have 68 to 75 seconds of steady window out of 90.
"MB/s" means ingress (bytes received on the client ports) for in-memory cells
and appended bytes for durable cells. "Cores" is the median per-second CPU of
the broker process. Kernel time spent on the broker's threads, including
softirq, is counted there too.

### Fairness checks

Two checks catch a cell where the load generators, not the broker, set the
number. The first is the busiest generator's peak CPU. Across the session A
cells on this page it ranged from 16% to 78% busy, so no generator was out of
CPU. The second is the ratio of the fastest generator's rate to the slowest.
It stayed between 1.04 and 1.61. A ratio near 1 means the broker served the
generators evenly. A higher one is still a valid broker figure, because the
broker-side measurement does not depend on how the load was split.

### Why per-generator client rates are never summed

Each generator reports its own MB/s over its own run. Adding those up assumes
they all ran over the same seconds at a steady rate. They don't. A generator
that starts early has the broker to itself for a moment, and one that finishes
early leaves the others a faster tail. The sum then reports a rate the broker
never sustained in any single second. Early in the campaign this inflated
results enough that those numbers were withdrawn. Starts were also staggered
by several seconds before `--start-at` existed (#723, fixed in #900). So this
page quotes only the broker's steady-state rate. A client figure appears only
where the broker has no counter for it, and it is labelled as such.

## Where the ceilings are

![Throughput and broker CPU against listener count, in memory and durable, at MTU 1500 and 3900](/felix/charts/perf-v060/listeners.svg)

On one 8 vCPU broker, Felix has two ceilings. In memory it is CPU-bound at
about 4.1 GB/s. Durable with `on_commit` it is disk-bound at about 1.43 GB/s.

### In memory: CPU-bound at about 4.1 GB/s

With four listeners, MTU 3900 and a client ACK threshold of 64, the
in-memory cell reached 4071.7, 4106.8 and 4119.7 MB/s in three trials, a mean
of 4099 MB/s (`best-inmem-l4`). The broker process used 7.38 to 7.40 cores of
8 the whole time. The generators peaked at 65% to 72% busy, so they had room
to send more. The broker had no CPU left to take it. That is what CPU-bound
means here.

### Durable `on_commit`: disk-bound at about 1.43 GB/s

The durable cells with the same settings appended 1413 to 1437 MB/s at 1, 2,
4 and 8 listeners (`best-dur-l1` through `best-dur-l8`, three trials each).
At four listeners the mean is 1432 MB/s, 97% of fio's best result on the same
array (1483.1 MB/s with 4 jobs). It is flat across listener counts, and the
broker used only 4.0 to 4.5 cores of 8. More listeners add CPU and network
capacity, but neither is the limit. The disk is.

This is a change from earlier pages, which found durable and in-memory
throughput equal. They were equal then because both sat below the network
and CPU ceiling. Now that the in-memory path reaches 4.1 GB/s, the durable
path is limited by how fast the array accepts synced writes.

On build `167120f0` at MTU 1500, durable at four listeners reached 1181 to
1197 MB/s while using 6.4 cores (`l557-l4-io0-dur`). There the CPU cost of
receiving the data held it below the disk. The changes described below cut
that cost, so the same disk is now the limit at 4.1 cores.

### The CPU cost model

![Broker cores per GB/s of ingress by category, from four perf profiles](/felix/charts/perf-v060/cost-model.svg)

A profile taken during the in-memory run shows where the CPU goes. The
category of each sample comes from `scripts/perf/azure/fold_categories.py`,
and the cores come from the cell's steady-state CPU. In the best
configuration (`prof-best-l4`, 4074 MB/s at 7.44 cores) the broker spends
1.83 cores per GB/s of ingress:

| Category | Share of samples | Cores per GB/s |
|---|---|---|
| Kernel: UDP receive syscall, including the copy to user space | 26.7% | 0.49 |
| Kernel: NIC receive softirq | 24.5% | 0.45 |
| QUIC packet encryption (AES-GCM) | 12.2% | 0.22 |
| Broker publish path and scheduling | 11.6% | 0.21 |
| quinn endpoint and connection drivers | 9.0% | 0.17 |
| quinn-proto packet processing | 8.7% | 0.16 |
| tokio runtime and park | 2.5% | 0.05 |
| UDP send (acks) and other kernel | 1.8% | 0.03 |
| memcpy and malloc | 1.5% | 0.03 |
| tracing, metrics, wire codec, other | 1.4% | 0.03 |

About half of the CPU is the kernel receiving UDP. Most of the rest is QUIC
work per packet: decrypting it, parsing it and routing it to its connection.
Felix's own code, the broker publish path, is about a ninth. To raise the
in-memory ceiling further, the broker has to handle fewer packets per byte or
spend less on each packet. Making Felix's own code faster would not move it
much.

The same model shows what each change removed. Going from `prof-l4-inmem`
(build `167120f0`, MTU 1500) to `prof-801-l4` (#905) cut the total from 4.03 to
3.37 cores per GB/s, mostly in the broker publish path (0.46 to 0.25) and in
quinn-proto (0.64 to 0.41). Moving to MTU 3900 and an ACK threshold of 64
(`prof-best-l4`) cut it to 1.83. Most of that came from the kernel receive path
and from per-packet QUIC work. Both changes are described under
[What we tried](#what-we-tried).

### Batched and unbatched cost per record

![Broker core-microseconds per record for batched and unbatched publishes](/felix/charts/perf-v060/per-record.svg)

The cost model above is per byte, measured with large batches. Unbatched
publishes cost much more per record. These cells used four listeners, MTU
3900, 48 keys, and acked publishes with 64 batches in flight per publisher
(`ab-felix-*-k48-t1`, one trial each). Cost per record is the broker's cores
divided by its records per second, taken from the broker's publish-byte
counter:

| Cell | Records/s | Broker cores | Core-µs per record |
|---|---|---|---|
| in memory, 4 KiB, batch 64 | 945,554 | 7.49 | 7.9 |
| in memory, 4 KiB, batch 1 | 298,782 | 7.27 | 24.3 |
| in memory, 256 B, batch 64 | 9,568,027 | 7.42 | 0.8 |
| in memory, 256 B, batch 1 | 429,379 | 7.35 | 17.1 |
| on_commit, 4 KiB, batch 64 | 324,315 | 3.79 | 11.7 |
| on_commit, 4 KiB, batch 1 | 65,128 | 5.53 | 84.9 |
| on_commit, 256 B, batch 64 | 2,267,582 | 4.30 | 1.9 |
| on_commit, 256 B, batch 1 | 95,314 | 5.87 | 61.5 |

Batched, the cost tracks the bytes: a 256 B record costs about a tenth of a
4 KiB one. Unbatched, it barely depends on size. In memory, a 256 B record
costs 17.1 µs and a 4 KiB record 24.3 µs, so a fixed cost per publish
dominates. Unbatched durable publishes cost 60 to 85 µs each, about 3.5 times
the in-memory figure for the same record. The transport is the same in both
cases, so most of that extra ~60 µs sits in the durable publish path itself.
The next section covers that path.

## Unbatched publish cost

:::caution[TODO(data)]
TODO(data): the profile of unbatched publishes (`prof-b1-dur-p4096`,
`prof-b1-dur-p256`, `prof-b1-inmem-p4096`), the root cause of the ~60 µs
extra per durable publish, and the before/after A/B of the fix. Findings so far:
unbatched in memory costs about 24 µs per 4 KiB record and unbatched durable
about 85 µs (table above). A code read lists suspects that the profile has to
confirm or rule out: a separate socket write per ack, several task hand-offs
per publish, wakeups after fsync that run one at a time, and per-publish
allocations and metric lookups.
:::

## What we tried

![Throughput and MB/s per core for each A/B on the four-listener in-memory cell](/felix/charts/perf-v060/experiments.svg)

Each experiment changed one thing on session A's four-listener in-memory cell
and ran three trials, interleaved with a control where possible. The measure
that matters is MB/s per broker core, because the broker is CPU-bound in this
cell.

| Arm | Cells | MB/s (mean of 3) | MB/s per core | Result |
|---|---|---|---|---|
| `167120f0` | `e1-base` | 1893 | 249 | baseline |
| #905, per-record copy removed | `e1-801` | 2158 | 300 | +14% MB/s, +20% per core, kept |
| #905 + client ACK threshold 64 | `e2-ackelicit64` | 2214 | 313 | +2.6%, +4.3% per core |
| #905, control for mimalloc | `e7-801` | 2156 | 299 | control |
| #905 + mimalloc | `e7-mimalloc` | 2162 | 304 | +0.3%, +1.7% per core, dropped |
| #905 + MTU 3900 | `e8-mtu3900` | 3995 | 537 | +85%, +80% per core |
| #905 + MTU 3900 + ACK threshold 64 | `best-inmem-l4` | 4099 | 555 | best profile |

### The per-record copy (#905)

Every binary publish byte was copied twice after quinn delivered it. The codec
copied it into a scratch buffer, then decoding copied each record into its own
`Vec<u8>`, one allocation per record (#902). #905 makes each record a slice of
its frame and reads frame bodies with `read_chunks`, 32 chunks per call. On
the four-listener cell it raised throughput from 1893 to 2158 MB/s and MB/s
per core from 249 to 300 (`e1-base`, `e1-801`). The profile matches. The
broker publish path dropped from 0.46 to 0.25 cores per GB/s and memcpy and
malloc from 0.13 to 0.04. quinn-proto dropped from 0.64 to 0.41, which fits
the reader taking the connection lock once per 32 chunks rather than once
per packet.

At one listener the fix changes nothing: 813 to 829 MB/s on `167120f0`
(`l557-l1-io0-inmem`) and 829 to 834 MB/s on `801fc22d` (`e8-mtu1500-l1`).
One listener is limited by its endpoint task, not by per-byte cost. See
[Listener count](#listener-count).

### The subscriber batcher (#719, #901)

Subscriber delivery took about 6 ms at p50 for small messages, while the
publish ack took under 0.2 ms. The lane feeder restarted its batch delay on
every message, so a stream faster than one message per delay waited until 64
events or 64 KiB had built up. On session B, RF=1, in memory, one publisher,
the ack p50 was 187 to 193 µs and delivery p50 was 6.1 to 6.4 ms for 0 B and
256 B records (`b375-inmem-lat-p0`, `b375-inmem-lat-p256`). For 4 KiB records
delivery was 2.1 ms, because 16 records fill the 64 KiB cap first
(`b375-inmem-lat-p4096`). Session C3's RF=3 Leader cells show the same 5.9 to
6.1 ms (`c425-rf3-leader-commitack-lat-p0`). #901 makes the delay a deadline
for the whole batch. The cells above are the before. The after is in
[Latency](#latency).

### io_uring submit errors (#903)

With io_uring flushing on, a failed `io_uring_enter` dropped the files of
flushes that were still in flight, so a later sync could hit a closed or
reused file descriptor (#722). #903 keeps each file open until its completion
is reaped, and retries `EAGAIN` and `EBUSY` instead of failing every
outstanding flush. This is a correctness fix. It was not A/B tested on its
own. Every `801fc22d` cell, including all the durable best-profile cells,
runs with it.

### mimalloc: no gain

The profile of `167120f0` put memcpy and malloc at 3.2% of samples, with more
hidden in unsymbolized libc frames under the payload copies (#721), so a
faster allocator looked worth a try. By the time it
ran, #905 had removed the per-record allocation. Three interleaved trials gave
2162 MB/s against 2156 for the control, +0.3%, and 304 against 299 MB/s per
core, +1.7% (`e7-mimalloc`, `e7-801`). In `prof-801-l4`, memcpy and malloc
together are 1.2% of samples, so even a free allocator could save little more
than that. We dropped it.

### MTU 1500 against 3900

Raising the NIC MTU from 1500 to 3900 on the broker and generators, with
`FELIX_MTU_UPPER_BOUND=3872` on both sides (3900 less 28 bytes of IPv4 and UDP
header), nearly doubled the in-memory ceiling. At four listeners it went from
2156 to 3995 MB/s at the same CPU, 299 to 537 MB/s per core (`e7-801`,
`e8-mtu3900`). At one listener it went from 830 to 1724 MB/s
(`e8-mtu1500-l1`, `e8-mtu3900-l1`).

The reason is the cost per packet. Most of the broker's CPU, from the NIC
softirq to decryption to quinn's parsing, is spent once per packet,
whatever its size. At MTU 3900 each packet carries about 2.6 times as much
data. Each datagram the broker received, after the kernel's GRO coalescing,
grew from about 11.5 KB to 27.9 KB at four listeners. Per GB/s, from
`prof-801-l4` to `prof-best-l4`, the NIC softirq dropped 55%, the receive
syscall 45%, quinn-proto 61%, AES-GCM 28% and the UDP send path 68%. AES-GCM
fell least because encryption still has to touch every byte.

There is a ceiling on how far this goes. On Linux, quinn sends up to 10
packets per GSO batch, and a batch is one UDP datagram limited to 65,507
bytes. So Felix clamps `FELIX_MTU_UPPER_BOUND` to 6550 and defaults to 4096
(`crates/protocol/felix-transport/src/config.rs`). Above 6550 the kernel
rejects every batch. A NIC with a 9000 MTU would still run Felix at 6550 at
most.

MTU 3900 helps only where every hop carries it. On Azure that means traffic
inside a VNet or between peered VNets in the same region. Clients across the
internet stay at 1500, so a deployment should expect the MTU 1500 figures for
them.

### The client ACK threshold

`FELIX_ACK_ELICITING_THRESHOLD` sets how many packets a QUIC receiver may take
in before it has to send an ACK. The default is 20. Setting 64 on the
generators gave +2.6% MB/s and +4.3% MB/s per core at MTU 1500
(`e2-ackelicit64` against `e1-801`), and +2.6% at MTU 3900 (`best-inmem-l4`
against `e8-mtu3900`). The broker sends fewer ACK datagrams. The default is
unchanged as of this draft.

### Listener count

One listener tops out near 830 MB/s at MTU 1500 with most of the broker idle
(3.1 cores of 8, `l557-l1-io0-inmem`). Every datagram for a port passes
through one quinn endpoint task. In the one-listener profile
(`prof-l1-inmem`), that task alone used 0.87 cores, so it was nearly
saturated (#557). More listeners mean more endpoint tasks. At MTU 1500,
2 listeners gave 1329 to 1336 MB/s and 4 gave 1890 to 1907
(`l557-l2-io0-inmem`, `l557-l4-io0-inmem`), at which point the broker used
7.3 to 7.5 cores.

Eight listeners lose to four on this 8 vCPU machine. At MTU 3900, eight
listeners gave 3430 to 3455 MB/s against 4072 to 4120 for four, 16% less
while using more CPU, 7.73 cores against 7.39 (`best-inmem-l8`,
`best-inmem-l4`). The profiles show where the difference is. Per GB/s, from
four to eight listeners (`prof-best-l4`, `prof-best-l8`), AES-GCM went from
0.22 to 0.43 cores, quinn-proto from 0.16 to 0.39 and quinn's drivers from
0.17 to 0.25. Kernel receive work did not rise: the syscall went from 0.49 to
0.41 and softirq from 0.45 to 0.41. So the extra cost is in user-space
compute, not syscalls or wakeups. The same work takes about twice as long
per byte.

Our leading explanation, **not yet confirmed**, is SMT. If the 8 vCPUs are 4
physical cores with two hardware threads each, eight busy listener threads
share cores, and compute-heavy work such as AES-GCM and packet parsing runs
slower on each. We did not record `lscpu` on the broker, so the core layout
is unverified. The eight-listener profile also has more unsymbolized stacks
(16.4% against 9.2%). Pinning threads, or comparing four and eight listeners
on a 16 vCPU VM, would settle it. Durable throughput does not care: it was
1413 to 1437 MB/s at every listener count, because the disk is the limit.

### Fewer runtime threads

On `167120f0` at MTU 1500, four listeners with `FELIX_IO_RUNTIME_THREADS=6`
gave 1616 to 1637 MB/s at 5.65 cores, against 1890 to 1907 MB/s at 7.3 to 7.5
cores with the default (`l557-l4-io6-inmem`, `l557-l4-io0-inmem`). That is
14% less throughput for 13% more MB/s per core. The default stays, because
peak throughput is what this cell measures.

### The default listener count (#918)

Based on the sweep, #918 sets the default `FELIX_QUIC_LISTENERS` to
`max(1, min(cores / 2, 4))`, so an 8 vCPU broker binds 4. An explicit value
still wins. The derived count stops before the internal port. A cluster member
on the default ports (client 5000, internal 5001) therefore keeps one
listener until it moves its internal port or sets the count. The Helm chart
also keeps setting the count explicitly, because it has to list every port.

## Replication

:::caution[TODO(data)]
TODO(data): session C3's RF=3 rows (Leader and Quorum, ack on commit, lease and
lease-free, in memory and durable) against session B's RF=1 rows, from the
final run (#425). Session C3's first Quorum latency cells failed while
token renewal was broken (see [Harness lessons](#harness-lessons)) and are
being rerun. All of B and C3 is limited by the P10 disks for durable cells, so
those rows compare replication modes, not peak throughput.
:::

## Payload and batch shapes

:::caution[TODO(data)]
TODO(data): the payload × batch × acked grid (256 B, 1 KiB and 4 KiB; batch 1
and 64; fire-and-forget and 64 in flight), in memory and durable, on session A
at the best profile with keys spread over all shards. Session B's shape cells
ran with the key skew described below and are not used.
:::

## Latency

:::caution[TODO(data)]
TODO(data): acked-publish and publish-to-delivery latency, p50/p99/p999, after
#901 on main, for in memory, Periodic and OnCommit, RF=1 and RF=3. The before
numbers are in [The subscriber batcher](#the-subscriber-batcher-719-901).
:::

## NATS JetStream comparison

:::caution[TODO(data)]
TODO(data): results of the tuned, interleaved Felix and NATS JetStream pairs at
MTU 3900 and MTU 1500, as throughput, broker CPU per message, and latency.
The runs are still in progress. No NATS result is published until the
fairness review below is complete.
:::

### Conditions

:::caution[TODO(data)]
TODO(data): fill in every item below before any result is shown.

- **Versions.** Felix commit, nats-server and client versions, load generators.
- **Hardware and network.** The same broker VM and NVMe array, generators and
  VNet for both; MTU 3900 and 1500; TLS on for both.
- **Durability, side by side.** What each pairing guarantees when the ack
  arrives: Felix in memory and Periodic against JetStream's default sync, and
  Felix `on_commit` against JetStream `sync_interval: always`. State which
  side syncs per message and which per batch.
- **Tuning.** NATS's published server, client and kernel tuning, the sweep
  that picked its best configuration (streams, publish mode, in-flight window,
  clients), and the Felix best profile.
- **Pairing rules.** Matched payloads, key or stream counts (12 and 48),
  R1 against RF=1, the same windows and trials, alternating order, and the same
  CPU sampler.
- **Structural differences.** QUIC over UDP against TCP, batching semantics,
  Go against Rust. Disclosed, not adjusted for.
- **Commands and configs.** Exact commands, server configs and sysctls, so the
  comparison can be reproduced or challenged.
:::

## Harness lessons

These affect how to read the numbers from sessions B and C3, and why some
cells are not used.

**Sessions B and C3 are disk-bound on durable cells.** Their brokers use
128 GiB Premium SSDs, which Azure rates as P10: 100 MB/s and 500 IOPS. Durable
`b375-periodic-ingest-c12` appended 209 to 219 MB/s across three brokers, and
`b375-oncommit-ingest` 124 to 273 MB/s, while the same cells received 1.8 to 2.0 GB/s of
fire-and-forget traffic. Unbatched durable publishes on B hit the
commit timeout, with each fsync covering 1 to 3 records at 4 to 6 ms (#922).
None of this is comparable to session A's NVMe numbers. For B's durable rows,
read the append rate, not ingress.

**Twelve keys reached only 8 of 12 shards.** The load generator named its keys
`k0` to `k11`. Under the stream's routing hash those land on shards
`[6,10,10,5,2,7,4,4,6,9,3,6]`: four shards idle and three doubled. On the
three-broker sessions this loads the brokers unevenly. #924 picks key names
that cover every shard evenly. Session A has a single broker that owns every
shard, so the skew does not change its placement.

**A 90-second cell ran for 1178 seconds (#922).** The load generator checked
its deadline only between sends, and a fire-and-forget send could wait behind
a durable backlog. It also counted sends, not appends. #924 bounds every send
by the deadline and reports acked records separately. Broker-side steady state
was not affected.

**Publish connections can buffer far more than the ingress budget (#923).**
The client listener uses the cache transport's receive windows, 64 MiB per
stream and 256 MiB per connection, while the ingress budget is 16 MiB per
connection. On B, two brokers held 948 MB and 753 MB of unread publishes and
took about 20 minutes to drain. This is open.

**Tokens expired mid-session.** Session control planes issue 8-hour tokens,
and runs longer than that failed with 401s. On one night an automatic OS
upgrade also restarted the in-memory control planes and lost their keys. Cells
that failed were rerun, and no partial cell is quoted. #912 switched the
harness to refresh tokens, and #914 fixed the file mode of the rotated
token.

## Reproducing

The charts on this page come from the raw cells:

```bash
pip install -r scripts/perf/requirements.txt   # matplotlib
python3 scripts/perf/v060_charts.py --sessions scripts/perf/azure/sessions
```

It writes the SVGs and `data.csv` to `docs-site/public/charts/perf-v060/`.
`data.csv` lists every plotted value with the cell it came from. To get the
profile breakdown for one cell:

```bash
python3 scripts/perf/azure/fold_categories.py \
  <cell>/felixperf-broker-0.folded.gz --cores <ss_cores> --mbs <ss_ingress_mb_s>
```
