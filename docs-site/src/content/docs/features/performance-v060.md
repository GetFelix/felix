---
title: "v0.6.0 Performance Review"
description: "Where Felix's throughput ceilings are on Azure, what sets them, what we changed to raise them, how Felix compares with NATS JetStream on the same machines, and how the numbers were measured."
---

:::caution[Draft]
This page is a draft for review. The method, ceilings, experiments, NATS
comparison and harness lessons use final data from the runs that finished.
The Azure runs stopped on 2026-10-02 when the subscription reached its spending
limit. Every gap that left is marked "In progress" and resumes on 2026-10-21,
when the subscription reactivates.
:::

This review covers the v0.6.0 performance campaign on Azure. It says where
Felix's throughput stops, what stops it, which changes moved the limit and
which did not, and how Felix compares with NATS JetStream on the same
hardware. Every number has a footnote that names the run it came from. The
runs, which the harness calls cells, are listed by section in the
[appendix](#appendix-runs-behind-each-section). The charts are generated from
the raw runs by `scripts/perf/v060_charts.py`.

## Headline numbers

:::note[In progress: resumes 2026-10-21]
The final confirmation run on main, with every merged change, replaces the
numbers below. Until then they are the best measured so far, on the builds
named in each row.
:::

All rows are from the NVMe broker described under [Setups](#setups), at MTU
3900 with four listeners.

| What | Result | Build | Runs |
|---|---|---|---|
| In memory, 4 KiB records in batches of 64, fire-and-forget | 4,099 MB/s, CPU-bound at 7.4 of 8 cores[^h-inmem] | #905 | 3 |
| Durable `on_commit`, 4 KiB × 64 | 1,432 MB/s, 97% of the disk's fio result[^h-dur] | #905 | 3 |
| In memory, 256 B × 64, acked | 9.57 million records/s[^h-256] | #905 | 1 |
| Durable `on_commit`, 256 B × 64, acked | 2.27 to 2.29 million records/s[^h-256] | #905 | 3 |
| One 256 B publish in flight, in memory: ack / delivery p50 | 175 to 204 µs / 175 to 178 µs[^h-lat] | #927 | 2 |
| One 256 B publish in flight, `on_commit`: ack / delivery p50 | 347 to 351 µs / 345 to 347 µs[^h-lat] | #927 | 2 |

Against NATS JetStream 2.15.0 on the same broker, Felix is ahead wherever the
broker has to fsync before it acks, and ahead or level everywhere else except
one case. Core NATS publishes 256 B messages with no ack about 14% faster than
Felix's fire-and-forget publish. See
[NATS JetStream comparison](#nats-jetstream-comparison).

## Test setup and method

### Setups

Three setups ran, each in its own Azure region and resource group. This page
calls them by the names in the first row.

| | NVMe broker | RF=1 cluster | RF=3 cluster |
|---|---|---|---|
| Region | eastus2 | westus3 | centralus, zones 1, 2 and 3 |
| Brokers | 1 × `Standard_L8as_v4` (8 vCPU, 64 GiB) | 3 × `Standard_D4as_v5` (4 vCPU each), RF=1 | 3 × `Standard_D4as_v5`, RF=3 |
| Broker data disk | 4 local NVMe drives, RAID0 (`md0`), ext4 | 128 GiB Premium SSD (P10), ext4 | 128 GiB Premium SSD (P10), ext4 |
| Generators | 4 × `Standard_D4as_v5` | 2 × `Standard_D4as_v5` | 2 × `Standard_D4as_v5` |
| Control plane | `Standard_D2as_v5` | `Standard_D2s_v5` | `Standard_D2s_v5` |
| MTU | 1500, then 3900 from the MTU experiment on | 1500 | 1500 |

Every VM ran kernel `6.17.0-1022-azure` with accelerated networking. On the
NVMe broker and its generators the NIC is a Mellanox `mlx5_core` virtual
function behind `hv_netvsc`.[^nic] The NICs start at MTU 1500. From the MTU
experiment onward, the NVMe broker and its generators ran at MTU 3900, with a
`ping -M do` path check from each generator.[^mtu-check]

### Builds

| Build | What it is | Used for |
|---|---|---|
| `167120f0` | main before the per-record copy fix (#905) | the "before" runs, and everything on the two clusters |
| `801fc22d` | the merge of #905, which also contains #901 and #903 | the NVMe broker's best profile and the NATS pairs |
| `a7146d1b` | main with #924, the base of the #927 A/B | latency before #927 |
| `2cd120e7` | the head of #927's branch, merged as `efc43abf` | latency after #927 |

This page names a build by the change it adds: "before #905", "#905", "before
#927" and "#927". The profile runs used frame-pointer builds of the same
commits. The NVMe broker and the RF=1 cluster used the load generator from
`perf-loadgen-window@16321bec`. The RF=3 cluster used `main@13b6003e`, which
sends each publish straight to the broker that leads its shard.

The NVMe broker's disk baseline, from `fio` with sequential writes and
`fdatasync` after each:[^fio]

| fio test | MB/s | sync p50 | sync p99 |
|---|---|---|---|
| 256 KiB, 1 job | 1426.8 | 71 µs | 2.6 ms |
| 256 KiB, 4 jobs | 1483.1 | 97 µs | 5.5 ms |
| 256 KiB, 8 jobs | 1477.0 | 235 µs | 9.2 ms |
| 4 KiB, 1 job | 51.3 | 95 µs | 117 µs |

:::note[In progress: resumes 2026-10-21]
A fio baseline on the two clusters' P10 data disks. The only fio file from
those clusters was recorded before the data disks were attached and measured
the OS disk, so it is not quoted. Until then this page uses Azure's rating
for P10: 100 MB/s and 500 IOPS.
:::

### Workload

Unless a section says otherwise, the NVMe broker's throughput runs use this
workload. Each of the four generators runs 16 publishers that send 4 KiB
records in batches of 64, keyed over 12 keys, for 90 seconds. Publishes are
fire-and-forget, and the broker runs with `FELIX_PUB_INGRESS_WAIT=1`, so a full
ingress budget slows the publisher down instead of dropping. Broker and
generators use 24 MiB UDP socket buffers. The broker runs with
`FELIX_STORAGE_IO_URING=1` and `FELIX_ACK_ON_COMMIT=1`, so a durable ack always
means the record is on disk. The in-memory runs write to stream `perf`, the
durable ones to `perf-durable` with `on_commit` fsync.

### How steady state is measured

All generators get the same start time (`--start-at`) and the same duration,
so they overlap for almost the whole run. The headline figure for a run
comes from the broker, not the clients. The broker's agent samples its
counters once a second: appended bytes, publish bytes, bytes received on each
client port, UDP datagrams and drops, and the broker process's CPU ticks.
`scripts/perf/azure/summarize.py` then does the following:

1. It takes the window when every generator was running: from the last
   generator's start to the first generator's end.
2. It drops the first and last 10% of that window.
3. It cuts the rest into one-second steps, sums each step over brokers, and
   reports the median step.

Most runs on this page have 68 to 75 seconds of steady window out of 90.
"MB/s" means ingress (bytes received on the client ports) for in-memory runs
and appended bytes for durable runs. "Cores" is the median per-second CPU of
the broker process. Kernel time spent on the broker's threads, including
softirq, is counted there too.

### Fairness checks

Two checks catch a run where the load generators, not the broker, set the
number. The first is the busiest generator's peak CPU. Across the NVMe
broker's Felix runs on this page it ranged from 16% to 78% busy, so no
generator was out of CPU. The second is the ratio of the fastest generator's
rate to the slowest. It stayed between 1.04 and 1.61. A ratio near 1 means
the broker served the generators evenly. A higher one is still a valid broker
figure, because the broker-side measurement does not depend on how the load
was split.

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
in-memory run reached 4071.7, 4106.8 and 4119.7 MB/s in three trials, a mean
of 4099 MB/s.[^h-inmem] The broker process used 7.38 to 7.40 cores of 8 the
whole time. The generators peaked at 65% to 72% busy, so they had room to
send more. The broker had no CPU left to take it. That is what CPU-bound
means here.

### Durable `on_commit`: disk-bound at about 1.43 GB/s

The durable runs with the same settings appended 1413 to 1437 MB/s at 1, 2,
4 and 8 listeners, three trials each.[^h-dur] At four listeners the mean is
1432 MB/s, 97% of fio's best result on the same array (1483.1 MB/s with 4
jobs). It is flat across listener counts, and the broker used only 4.0 to 4.5
cores of 8. More listeners add CPU and network capacity, but neither is the
limit. The disk is.

This is a change from earlier pages, which found durable and in-memory
throughput equal. They were equal then because both sat below the network
and CPU ceiling. Now that the in-memory path reaches 4.1 GB/s, the durable
path is limited by how fast the array accepts synced writes.

Before #905, at MTU 1500, durable at four listeners reached 1181 to 1197 MB/s
while using 6.4 cores.[^old-dur] There the CPU cost of receiving the data
held it below the disk. The changes described below cut that cost, so the
same disk is now the limit at 4.1 cores.

### The CPU cost model

![Broker cores per GB/s of ingress by category, from four perf profiles](/felix/charts/perf-v060/cost-model.svg)

A profile taken during the in-memory run shows where the CPU goes. The
category of each sample comes from `scripts/perf/azure/fold_categories.py`,
and the cores come from the run's steady-state CPU. In the best
configuration (4074 MB/s at 7.44 cores) the broker spends 1.83 cores per GB/s
of ingress:[^prof-best]

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

The same model shows what each change removed. Going from the build before
#905 at MTU 1500 to #905 cut the total from 4.03 to 3.37 cores per GB/s,
mostly in the broker publish path (0.46 to 0.25) and in quinn-proto (0.64 to
0.41).[^prof-steps] Moving to MTU 3900 and an ACK threshold of 64 cut it to
1.83. Most of that came from the kernel receive path and from per-packet QUIC
work. Both changes are described under [What we tried](#what-we-tried).

### Batched and unbatched cost per record

![Broker core-microseconds per record for batched and unbatched publishes](/felix/charts/perf-v060/per-record.svg)

The cost model above is per byte, measured with large batches. Unbatched
publishes cost much more per record. These runs used four listeners, MTU
3900, 48 keys, and acked publishes with 64 batches in flight per publisher,
one trial each.[^per-record] Cost per record is the broker's cores divided by
its records per second, taken from the broker's publish-byte counter:

| Run | Records/s | Broker cores | Core-µs per record |
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

An unbatched in-memory publish costs the broker about 24 µs of CPU at 4 KiB
and 17 µs at 256 B. The same publish to a durable `on_commit` stream costs
about 85 µs at 4 KiB and 62 µs at 256 B (table above). The QUIC receive path
is the same for both, so the extra 45 to 60 µs is per-publish work in the
durable path.

A code read lists the suspects. Each durable publish does its own
`begin_append` and `write(2)`, takes its own turn in the commit order, spawns
a completion task, waits for the commit, and builds its own fanout envelope.
Around that sit several task hand-offs in the publish scheduler. After an
fsync the waiters wake one at a time, because they queue on a FIFO mutex. Each
ack is its own socket write, and each publish arms its own timeout timer and
does several metric lookups and allocations.

#932 (a draft) attacks the largest of these. When a worker takes a durable
publish, it also takes the plain durable publishes queued behind it on the
same lane, up to 64 publishes or 1 MiB, and claims them together. The group
then costs one write, one commit turn, one spawned task, one commit wait and
one fanout envelope. Each publish keeps its own offset and its own answer,
sent in lane order once the group is durable. #932 also wakes every waiter
that a flush covers at once, instead of one by one.

:::note[In progress: resumes 2026-10-21]
The profiles of unbatched publishes, to confirm or rule out each suspect, and
the A/B of #932 against main on the NVMe broker. Neither has run. Nothing on
this page claims #932 is faster yet.
:::

## What we tried

![Throughput and MB/s per core for each A/B on the four-listener in-memory run](/felix/charts/perf-v060/experiments.svg)

Each experiment changed one thing on the NVMe broker's four-listener in-memory
run and ran three trials, interleaved with a control where possible. The
measure that matters is MB/s per broker core, because the broker is CPU-bound
in this run.[^experiments]

| Arm | MB/s (mean of 3) | MB/s per core | Result |
|---|---|---|---|
| before #905 | 1893 | 249 | baseline |
| #905, per-record copy removed | 2158 | 300 | +14% MB/s, +20% per core, kept |
| #905 + client ACK threshold 64 | 2214 | 313 | +2.6%, +4.3% per core |
| #905, control for mimalloc | 2156 | 299 | control |
| #905 + mimalloc | 2162 | 304 | +0.3%, +1.7% per core, dropped |
| #905 + MTU 3900 | 3995 | 537 | +85%, +80% per core |
| #905 + MTU 3900 + ACK threshold 64 | 4099 | 555 | best profile |

### The per-record copy (#905)

Every binary publish byte was copied twice after quinn delivered it. The codec
copied it into a scratch buffer, then decoding copied each record into its own
`Vec<u8>`, one allocation per record (#902). #905 makes each record a slice of
its frame and reads frame bodies with `read_chunks`, 32 chunks per call. On
the four-listener run it raised throughput from 1893 to 2158 MB/s and MB/s
per core from 249 to 300. The profile matches. The broker publish path
dropped from 0.46 to 0.25 cores per GB/s and memcpy and malloc from 0.13 to
0.04. quinn-proto dropped from 0.64 to 0.41, which fits the reader taking the
connection lock once per 32 chunks rather than once per packet.

At one listener the fix changes nothing: 813 to 829 MB/s before #905 and 829
to 834 MB/s with it.[^one-listener] One listener is limited by its endpoint
task, not by per-byte cost. See [Listener count](#listener-count).

### The subscriber batcher (#719, #901)

Subscriber delivery took about 6 ms at p50 for small messages, while the
publish ack took under 0.2 ms. The lane feeder restarted its batch delay on
every message, so a stream faster than one message per delay waited until 64
events or 64 KiB had built up. On the RF=1 cluster, in memory, with one
publisher, the ack p50 was 187 to 193 µs and delivery p50 was 6.1 to 6.4 ms
for 0 B and 256 B records. For 4 KiB records delivery was 2.1 ms, because 16
records fill the 64 KiB cap first. The RF=3 cluster's Leader runs show the
same 5.9 to 6.1 ms.[^batcher-before] #901 makes the delay a deadline for the
whole batch. The before and after are in [Latency](#latency).

### io_uring submit errors (#903)

With io_uring flushing on, a failed `io_uring_enter` dropped the files of
flushes that were still in flight, so a later sync could hit a closed or
reused file descriptor (#722). #903 keeps each file open until its completion
is reaped, and retries `EAGAIN` and `EBUSY` instead of failing every
outstanding flush. This is a correctness fix. It was not A/B tested on its
own. Every run on #905, including all the durable best-profile runs, has it.

### mimalloc: no gain

The profile before #905 put memcpy and malloc at 3.2% of samples, with more
hidden in unsymbolized libc frames under the payload copies (#721), so a
faster allocator looked worth a try. By the time it ran, #905 had removed the
per-record allocation. Three interleaved trials gave 2162 MB/s against 2156
for the control, +0.3%, and 304 against 299 MB/s per core, +1.7%. In the #905
profile, memcpy and malloc together are 1.2% of samples, so even a free
allocator could save little more than that. We dropped it.

### MTU 1500 against 3900

Raising the NIC MTU from 1500 to 3900 on the broker and generators, with
`FELIX_MTU_UPPER_BOUND=3872` on both sides (3900 less 28 bytes of IPv4 and UDP
header), nearly doubled the in-memory ceiling. At four listeners it went from
2156 to 3995 MB/s at the same CPU, 299 to 537 MB/s per core. At one listener
it went from 830 to 1724 MB/s.[^mtu]

The reason is the cost per packet. Most of the broker's CPU, from the NIC
softirq to decryption to quinn's parsing, is spent once per packet,
whatever its size. At MTU 3900 each packet carries about 2.6 times as much
data. Each datagram the broker received, after the kernel's GRO coalescing,
grew from about 11.5 KB to 27.9 KB at four listeners. Per GB/s, between the
#905 profiles at the two MTUs, the NIC softirq dropped 55%, the receive
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
generators gave +2.6% MB/s and +4.3% MB/s per core at MTU 1500, and +2.6% at
MTU 3900.[^ack] The broker sends fewer ACK datagrams. The default is unchanged
as of this draft.

### Listener count

One listener tops out near 830 MB/s at MTU 1500 with most of the broker idle
(3.1 cores of 8). Every datagram for a port passes through one quinn endpoint
task. In the one-listener profile, that task alone used 0.87 cores, so it was
nearly saturated (#557). More listeners mean more endpoint tasks. At MTU
1500, 2 listeners gave 1329 to 1336 MB/s and 4 gave 1890 to 1907, at which
point the broker used 7.3 to 7.5 cores.[^listeners]

Eight listeners lose to four on this 8 vCPU machine. At MTU 3900, eight
listeners gave 3430 to 3455 MB/s against 4072 to 4120 for four, 16% less
while using more CPU, 7.73 cores against 7.39.[^l8] The profiles show where
the difference is. Per GB/s, from four to eight listeners, AES-GCM went from
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

Before #905, at MTU 1500, four listeners with `FELIX_IO_RUNTIME_THREADS=6`
gave 1616 to 1637 MB/s at 5.65 cores, against 1890 to 1907 MB/s at 7.3 to 7.5
cores with the default.[^io6] That is 14% less throughput for 13% more MB/s
per core. The default stays, because peak throughput is what this run
measures.

### The default listener count (#918)

Based on the sweep, #918 sets the default `FELIX_QUIC_LISTENERS` to
`max(1, min(cores / 2, 4))`, so an 8 vCPU broker binds 4. An explicit value
still wins. The derived count stops before the internal port. A cluster member
on the default ports (client 5000, internal 5001) therefore keeps one
listener until it moves its internal port or sets the count. The Helm chart
also keeps setting the count explicitly, because it has to list every port.

### The subscriber event-batch delay: no effect

When the NATS latency pairs showed Felix delivering about 480 µs after its own
ack, the first suspect was `FELIX_EVENT_BATCH_MAX_DELAY_US`, which defaults to
250 µs. Setting it to 0 or 50 changed nothing: in-memory delivery p50 stayed
at 704 to 708 µs and `on_commit` at 794 to 824 µs, two trials each.[^evdelay]
The cause was elsewhere. See
[One message in flight](#one-message-in-flight).

## Latency

![Ack and delivery latency at 256 B before and after #927, against NATS JetStream](/felix/charts/perf-v060/latency-927.svg)

These runs keep one publish in flight: the next starts when the last is
acked. They report the time to the ack and the time until a subscriber on a
second connection receives the record. Each run measures 20,000 publishes
after 2,000 of warmup.

Delivery latency changed twice in this release. The table shows 256 B
records on the NVMe broker, in memory and with `on_commit`.

| Build | In memory: ack p50 | In memory: delivery p50 / p99 | `on_commit`: ack p50 | `on_commit`: delivery p50 / p99 |
|---|---|---|---|---|
| before #905 (old batcher) | 179 µs | 5,909 / 11,767 µs | 346 µs | 7,853 / 22,149 µs |
| before #927 (#901 batcher) | 180 to 181 µs | 704 to 709 / 1,260 to 1,267 µs | 344 to 348 µs | 793 to 821 / 1,377 to 1,429 µs |
| #927 | 175 to 204 µs | 175 to 178 / 213 to 248 µs | 347 to 351 µs | 345 to 347 / 445 µs |

The first row is one run each.[^lat-old] The other two are two runs each,
interleaved.[^lat-927] #901 removed the batch delay that restarted on every
message and took delivery from about 6 to 8 ms down to 0.7 to 0.8 ms. #927
removed a timer that rounded up to a whole millisecond and took it to the
ack's own latency. The ack did not change in either step.

The same 5.9 to 6.4 ms appears on both clusters before #901, which ran the
older build.[^batcher-before]

At 4 KiB and with periodic fsync, only the runs before #927 exist. On build
#905: periodic at 256 B acked in 199 µs p50 and delivered in 732 µs; periodic
at 4 KiB, 232 µs and 774 µs; `on_commit` at 4 KiB, 424 µs and 907
µs.[^lat-pre927] Each delivery figure is 480 to 540 µs behind its ack, the
same gap #927 removed at 256 B.

:::note[In progress: resumes 2026-10-21]
Latency after #927 at 4 KiB, with periodic fsync, and on both clusters.
:::

## Replication

The two clusters ran the build before #905, at MTU 1500 with one listener.
Their delivery figures include the old batcher, so this section compares
acks and throughput across replication modes, not delivery. The RF=3
cluster's load generator sends each publish to the broker that leads its
shard. The RF=1 cluster's load generator sends every publish through the
first broker, which relays it when another broker leads the shard. The
broker's counters show which runs were relayed.

### Ack latency by mode

One 256 B publish in flight, three trials each:[^repl-lat]

| Setup | Mode | Ack p50 | Ack p99 | Delivery p50 |
|---|---|---|---|---|
| RF=1 cluster | in memory | 189 to 193 µs | 220 to 270 µs | 6.27 to 6.37 ms |
| RF=1 cluster | periodic fsync, relayed | 448 to 478 µs | 540 to 569 µs | 2.59 to 3.39 ms |
| RF=1 cluster | `on_commit`, relayed | 4.02 to 4.50 ms | 10.8 to 14.4 ms | 5.31 to 5.82 ms |
| RF=3 cluster | Leader, ack on commit, in memory | 183 to 184 µs | 216 to 218 µs | 6.06 to 6.10 ms |
| RF=3 cluster | Leader, ack on enqueue, in memory | 181 to 184 µs | 213 to 228 µs | 5.98 to 6.11 ms |
| RF=3 cluster | Quorum, in memory | 454 to 475 µs | 526 to 540 µs | 1.50 to 2.25 ms |
| RF=3 cluster | Quorum, durable, periodic fsync | 2.61 to 2.70 ms | 4.22 to 4.77 ms | 3.50 to 3.62 ms |
| RF=3 cluster | Quorum, durable, `on_commit` | 22.7 to 23.2 ms | 32.6 to 34.5 ms | 23.9 to 24.4 ms |

Leader mode acks as fast as a single broker, because the leader answers
before the followers have the record. Quorum waits for a follower in another
zone, which adds about 270 to 290 µs. That is the cost of a quorum ack across zones
on these VMs.

The durable rows run on P10 disks. On the RF=1 cluster, each `on_commit`
publish waited for one fsync that averaged 4.1 to 4.6 ms, and the ack
followed it. On the RF=3 cluster each durable `on_commit` publish cost three
fsyncs, one per broker, averaging 4.7 to 5.0 ms. Its 23 ms ack is longer than
three of those in a row. We have not broken it down yet. These rows say what
the disk costs, not what Felix costs. On the NVMe broker the same `on_commit`
ack takes 347 µs.

### Fire-and-forget throughput by mode

4 KiB records in batches of 64, 24 publishers, in memory, three trials each.
MB/s is ingress summed over the three brokers:[^repl-tp]

| Setup | Mode | Ingress MB/s | Broker cores (of 12) |
|---|---|---|---|
| RF=1 cluster | Leader | 1940 to 1974 | 10.70 to 10.77 |
| RF=1 cluster | Quorum | 1915 to 1922 | 10.53 to 10.61 |
| RF=3 cluster | Leader, ack on commit | 1958 to 2021 | 10.96 to 11.11 |
| RF=3 cluster | Leader, ack on enqueue | 1955 to 1991 | 10.61 to 10.62 |
| RF=3 cluster | Quorum | 1924 to 1990 | 10.53 to 10.68 |

The five means sit within about 3% of each other, with the brokers near
their 12 cores. At this load the brokers are CPU-bound on receiving the data, and the
replication mode does not move that.

The durable Quorum runs are not in this table. Their clients offered about
2 GB/s of fire-and-forget traffic to brokers on P10 disks. The first trials
appended 10.8 to 12.3 MB/s on average with `on_commit` and 23.2 to 61.8 MB/s
with periodic fsync, and counted 513 to 654 failed quorum waits each. That
is an overloaded disk, not a throughput figure.[^repl-dur]

### Bugs found and fixed

**Quorum cache puts waited for the replication tick (#928).** A cache put on
a Quorum stream never told the replicator that it had appended. The record
went out on the next 2-second replication tick, so a single client saw about
840 ms per put. #929 signals the replicator when a cache write waits for its
quorum. Its regression test ships a put in 0.22 s with the fix and 5.22 s
without it.

**In-memory replicated streams could not open (#930).** Once a cluster had
finalized `generation_start`, the broker tried to write it to every shard
before opening it. An in-memory stream has no persisted history, so the write
failed and the shard never opened for writes. Promotion retried thousands of
times. This blocked the RF=3 lease-free runs. #931 skips the write for
in-memory streams, which take no log offsets and have nothing to fence, and
backs off the retry to at most 2 seconds.

:::note[In progress: resumes 2026-10-21]
The lease-free RF=3 rows, which #930 blocked. Quorum cache numbers after
#929. Acked durable throughput on both clusters. A rerun of this section on
main, so the delivery column reflects #901 and #927.
:::

## Payload and batch shapes

Two partial grids exist, both in memory with acked publishes and 64 batches
in flight, one trial each. Both clusters ran the build before #905 at MTU 1500
with one listener, and both used 12 keys, which reach only 8 of 12 shards
(see [Harness lessons](#harness-lessons)). Publishes that reached a broker
not leading their shard were relayed. The RF=1 cluster ran 16 publishers per
generator and the RF=3 cluster 12. So compare shapes within a column, not
across columns or with the NVMe broker's numbers.[^shapes]

| Payload | Batch | RF=1 cluster MB/s | RF=1 cores (of 12) | RF=3 Quorum MB/s | RF=3 cores (of 12) |
|---|---|---|---|---|---|
| 256 B | 1 | 11.9 | 7.18 | 7.8 | 5.08 |
| 256 B | 64 | 358.5 | 7.76 | 286.9 | 6.67 |
| 1 KiB | 1 | 39.6 | 7.20 | 25.1 | 5.19 |
| 1 KiB | 64 | 712.3 | 8.56 | 694.5 | 8.61 |
| 4 KiB | 1 | 136.5 | 7.31 | 92.5 | 5.58 |
| 4 KiB | 64 | 985.8 | 9.11 | not run | |

Batching matters more than payload size. At 256 B, batches of 64 move 30
times the bytes of single-record publishes on the RF=1 cluster for about the
same CPU.

The fire-and-forget shapes are left out. On the RF=3 cluster the generators
ran at 93% to 99% CPU in those runs, so they measured the generators. On the
RF=1 cluster the unbatched fire-and-forget runs hit the load generator bug in
#922. The durable shapes are left out because P10 disks bound them: an
unbatched durable publish waited on a 4 to 6 ms fsync covering one to three
records, and some runs hit the commit timeout.

:::note[In progress: resumes 2026-10-21]
The full grid on the NVMe broker at the best profile: 256 B, 1 KiB and 4 KiB;
batch 1 and 64; fire-and-forget and 64 in flight; in memory and durable; with
keys spread over every shard.
:::

## NATS JetStream comparison

![Records per second and server CPU per record for each Felix and NATS JetStream pair](/felix/charts/perf-v060/nats-pairs.svg)

We ran NATS JetStream on the NVMe broker's own machines, with the same
generators, and tuned it to its best before comparing. A comparison is only
worth publishing if NATS gets the same hardware and its own best settings, so
this section lists the conditions first.

### Conditions

| | Felix | NATS |
|---|---|---|
| Version | build #905 (`801fc22d`) | nats-server 2.15.0, `nats` CLI 0.5.0, pinned and checked against each release's SHA256SUMS |
| Server | the NVMe broker, 1 × `Standard_L8as_v4`, local NVMe RAID0 | the same VM and array; Felix stopped while NATS ran, and NATS stopped while Felix ran |
| Generators | 4 × `Standard_D4as_v5`, 16 publishers each | the same 4 VMs, 16 clients each unless tuning picked more |
| Network | MTU 3900, QUIC with TLS | MTU 3900, checked end to end per run, TCP with TLS |
| Kernel | 24 MiB UDP socket buffers | 25 MiB TCP buffer maximum, `somaxconn` 4096, no slow start after idle |
| Spread | 48 keys | 48 R1 streams |
| Starting state | wiped data directory, `fstrim`, page cache dropped | empty data directory, `fstrim`, page cache dropped |
| Order | alternating: Felix first on odd trials, NATS first on even ones | |
| Measure | records/s and CPU from the server's own counters, same steady-state window | the same sampler on `nats-server`, the same window |

Both systems are counted on records per second. For durable and JetStream
runs, the rate is the steady append rate divided by the run's mean stored
record size. That way neither system's per-record overhead counts as
throughput: Felix's record header, or the subject and metadata NATS stores
beside each message.

Some things cannot be matched. Felix speaks QUIC over UDP and NATS its own
protocol over TCP. Felix is Rust and NATS is Go. Felix clients authenticate
with a JWT at connect time and NATS ran without auth; that cost falls outside
the measured window. The harness that ran the NATS side is in #916.

**The generators were not the limit.** No generator passed 75% CPU in any
pair. Felix's peaked at 74.5% and NATS's at 52.1%. For core NATS at 256 B the
harness reran the pair with 2 and then 4 `nats bench` processes per
generator, because its saturation check read 88.7%. That reading was the
server VM, not a generator: the check takes the highest CPU of every VM in
the run, the server's included. That is a bug in the harness, to fix in #916.
The reruns still show the rate did not depend on the generators. Equal-length
calibration runs with 1, 2 and 4 processes per generator published 141.6,
138.5 and 140.4 million messages. The quoted run used 4 processes, with its
generators at 47% to 52%.[^core-procs]

**NATS was tuned to its best.** Before the pairs ran, a sweep varied the
fast-batch window, the clients per generator and the async publish window.
With the default sync and with memory streams, at 256 B and 4 KiB, the
settings the pairs used were within 1.2% of the best setting found, and
async publishing was always slower.[^tune] For `sync_interval: always`, more
streams helped (about 38,000 messages/s with 12 streams against 46,700 to
48,500 with 24), so the pairs used 48.[^streams] The fsync pair with batches
uses NATS's atomic batch, for the reason below, at 64 clients per generator,
which beat 16 and 128.[^atomic-tune]

### What each pair guarantees

Felix `on_commit` and NATS `sync_interval: always` both fsync before they ack.
That is the strict pair.

Felix periodic fsync syncs every 250 ms. NATS's default syncs every 2
minutes. Its configuration docs explain why: "By default JetStream relies on
stream replication in the cluster to guarantee data is available after an OS
crash." A single R1 server, as here, has no replicas, so the default pair
gives NATS a much longer window than Felix. The two are paired anyway,
because both sync on a timer rather than per message, and this is how NATS
runs a durable stream by default.

The NATS docs also warn that `always` "will slow down the throughput to a few
hundred msg/s". On this NVMe array it did much better, about 48,000 to 65,000
messages per second unbatched. The source shows why batching barely helps
under `always`. A JetStream fast batch still syncs each message: the store
calls `Sync()` for every write when sync is set to always
(`server/filestore.go` line 8777 at v2.15.0). An atomic batch coalesces the
sync to one per batch (`server/stream.go` lines 7889 to 7926). So the batched
fsync pair uses the atomic batch, which is NATS's best option there.

In-memory Felix streams are paired with JetStream memory streams. Both keep
the same number of records and drop the oldest. Felix fire-and-forget is
paired with core NATS publish, which has no ack and no stream. Core NATS
routes the message and drops it, since nothing subscribes, while Felix still
appends it to the in-memory stream.

### Results

Each row is one pair, run back to back on the same machines. Records/s and
CPU are server-side. The fsync pair with batches has two trials, and the
table shows trial 2. Every other pair has one trial.

| Pair | Felix records/s | Felix cores | NATS records/s | NATS cores | Felix ÷ NATS |
|---|---|---|---|---|---|
| fsync before ack, batches of 64, 4 KiB | 321,611 | 3.79 | 107,130 | 4.72 | 3.0× |
| fsync before ack, batches of 64, 256 B | 2,272,979 | 4.34 | 165,376 | 4.47 | 13.7× |
| fsync before ack, unbatched, 4 KiB | 65,046 | 5.53 | 48,663 | 4.05 | 1.34× |
| fsync before ack, unbatched, 256 B | 95,330 | 5.87 | 64,675 | 3.36 | 1.47× |
| periodic / NATS default, batches of 64, 4 KiB | 308,172 | 3.46 | 294,414 | 5.19 | 1.05× |
| periodic / NATS default, batches of 64, 256 B | 3,651,532 | 5.46 | 476,059 | 3.04 | see below |
| in memory, batches of 64, 4 KiB | 945,554 | 7.49 | 306,431 | 5.33 | 3.1× |
| in memory, batches of 64, 256 B | 9,568,027 | 7.42 | 586,563 | 3.19 | see below |
| in memory, unbatched, 4 KiB | 298,782 | 7.27 | 234,566 | 6.11 | 1.27× |
| in memory, unbatched, 256 B | 429,379 | 7.35 | 344,202 | 5.28 | 1.25× |
| no ack (core NATS), 4 KiB | 983,518 | 7.44 | 980,007 | 6.35 | 1.00× |
| no ack (core NATS), 256 B | 7,956,547 | 7.35 | 9,035,452 | 7.03 | 0.88× |

Sources for every row are in the footnote.[^pairs]

**Durable, fsync before ack, batched.** Felix appends 3.0 times as many 4 KiB
records and 13.7 times as many 256 B records, using fewer cores. Trial 1 gave
3.1× and 13.7×. The difference is group commit. Felix's flush covers every
publish waiting on it, so one fsync serves many batches. NATS's atomic batch
coalesces one fsync per batch per stream. With JetStream's fast batch
instead, NATS fsyncs every message and reached 52,736 records/s at 4 KiB and
65,591 at 256 B. Felix ran 6.1× and 35× faster than that, one trial each.
That fast-batch result is shown for completeness, not as the fair pair.[^fast]

**Durable, fsync before ack, unbatched.** Felix is 1.34× ahead at 4 KiB and
1.47× at 256 B. Per record, the CPU is about equal at 4 KiB (85.0 µs for
Felix, 83.3 µs for NATS), and NATS uses about 15% less at 256 B (61.5 against
52.0 µs). This is the per-publish cost from
[Unbatched publish cost](#unbatched-publish-cost).

**Periodic against NATS default, 4 KiB.** A tie: 308,172 against 294,414
records/s. Felix used about a third less CPU (3.46 cores against 5.19). Felix
appended 1271 MB/s, 86% of the array's fio result. NATS's memory streams reached
about the same rate at 4 KiB (306,431), so its 4 KiB figure may be set by
the server rather than the disk.

**Periodic against NATS default, 256 B, and in memory at 256 B.** Felix
reached 3.65 million records/s with periodic fsync and 9.57 million in
memory. NATS reached about 480,000 and 590,000. We do not quote a ratio yet.
In both NATS runs the server used only about 3 of its 8 cores and the
generators 18% to 24%, so neither side of NATS looked saturated. No client
setting changed that. The window, clients per generator and async publishing
all landed between 385,000 and 593,000.[^tune] The one knob left is the
stream count.

**In memory, batched, 4 KiB.** Felix is 3.1× ahead, and NATS used 5.33 cores.

**In memory, unbatched.** Felix is about 1.25× ahead at both sizes. The CPU
per record is close: 24.3 against 26.0 µs at 4 KiB, and 17.1 against 15.3 µs
at 256 B, where NATS is cheaper.

**No ack.** At 4 KiB it is a tie, about 980,000 records/s each, and NATS used
15% less CPU. At 256 B NATS wins: 9.04 million against 7.96 million records/s,
14% more. Both servers were near their 8 cores. Remember that core NATS
stores nothing here, while Felix appends to an in-memory stream.

### Where NATS wins or ties

NATS publishes 256 B messages with no ack 14% faster than Felix, and at 4 KiB
it matches Felix for 15% less CPU. Unbatched, NATS needs less CPU per message
at 256 B in both durable and in-memory runs, even though Felix moves more
messages. On latency, NATS acks sooner in every mode measured: about 70 µs
sooner with an fsync before the ack, and about 20 to 30 µs sooner with
periodic or default sync, as the next section shows. Felix's 4 KiB periodic result ties NATS's
default sync, which syncs far less often.

### One message in flight

The latency pairs use the same method on both sides: one publisher, one
subscriber on a second connection, one publish in flight, 2,000 warmup and
20,000 measured. NATS was measured with a port of Felix's latency client that
shares its timing and percentile code. Its subscriber is an ordered push
consumer, which JetStream fills only after storing the message, as Felix fans
out only after the append. One trial each.[^lat-pairs]

| Pair | Payload | Felix ack p50 / p99 | Felix delivery p50 / p99 | NATS ack p50 / p99 | NATS delivery p50 / p99 |
|---|---|---|---|---|---|
| `on_commit` / `always`, before #927 | 256 B | 347 / 411 µs | 825 / 1,407 µs | 278 / 355 µs | 280 / 374 µs |
| `on_commit` / `always`, #927 | 256 B | 347 to 351 / 444 to 449 µs | 345 to 347 / 445 µs | | |
| `on_commit` / `always`, before #927 | 4 KiB | 424 / 484 µs | 907 / 1,496 µs | 334 / 407 µs | 370 / 457 µs |
| periodic / default, before #927 | 256 B | 199 / 274 µs | 732 / 1,241 µs | 181 / 226 µs | 181 / 237 µs |
| periodic / default, before #927 | 4 KiB | 232 / 291 µs | 774 / 1,371 µs | 203 / 266 µs | 239 / 296 µs |
| in memory, #927 | 256 B | 175 to 204 / 220 to 231 µs | 175 to 178 / 213 to 248 µs | not run | not run |

The first pair showed a problem (#926). Felix delivered each record about
480 µs after its own ack, while NATS delivered right after its ack. Setting
the event-batch delay to zero changed nothing, as described under
[What we tried](#the-subscriber-event-batch-delay-no-effect). The cause was a
timer. After sending a lone event, the subscriber feeder waited for more with
a `tokio` timeout. Tokio's timers tick once a millisecond and round a
deadline up to the next tick, so a lone event waited 0 to 1 ms, about 500 µs
on average. A delay of zero still armed the timer, which is why changing the
setting did nothing.

#927 sends a lone event at once and arms no timer when the delay is zero. In
an interleaved A/B, `on_commit` delivery p50 went from 821 to 345 µs and p99
from 1,429 to 445 µs. In-memory delivery went from 704 to 178 µs. NATS
delivered in 280 µs with `always` and 181 µs with its default sync. Throughput
did not suffer. With four subscribers and unbatched publishes the rate was
34,070 to 34,490 messages/s with #927 and 34,320 to 34,690 without, and with
one subscriber and batches of 64 it was 108,740 to 109,390 against 108,320 to
110,680. In both cases the means differ by less than 1%.[^lat-927]

That leaves the ack. With `on_commit`, Felix acks a 256 B publish in about
347 µs and NATS in 278 µs, about 70 µs sooner. At 4 KiB the gap is about
90 µs. With periodic and default sync the acks are within 30 µs of each other. So
most of the gap is on the fsync path, and it matches the per-publish cost in
[Unbatched publish cost](#unbatched-publish-cost). #932 targets that path and
has not been measured. In memory, Felix's ack after #927 (175 to 204 µs) is
level with NATS's default-sync ack (181 µs). NATS's memory stream was not
measured.

### In progress

:::note[In progress: resumes 2026-10-21]
- The remaining latency pairs: everything at 4 KiB and with periodic fsync
  after #927, NATS memory streams, and core NATS as a floor.
- Trial 2 of every pair that has one trial, and trials 3 and 4 of the
  batched fsync pair, so each pair has alternating order both ways.
- The stream-count test: NATS with 96 and 192 streams at 256 B, default sync
  and memory. Until it runs, the 256 B periodic and in-memory gaps are not
  quoted as ratios.
- The pairs at MTU 1500, where TCP has full TSO and GRO and Felix loses its
  MTU 3900 advantage: no ack and in-memory unbatched, two trials each.
- The A/B of #932 against main, on throughput and on the ack gap.
:::

### What this comparison does not cover

It covers one server with R1 streams against one Felix broker at RF=1. It
does not cover NATS clustering or R3 streams, which is how NATS recommends
running durable streams. It does not cover fanout above one subscriber, or
consumers that ack. It does not cover JetStream's R1 `persist_mode: async`,
which NATS offers for R1 streams.

## Harness lessons

These affect how to read the numbers from the two clusters, and why some runs
are not used.

**The two clusters are disk-bound on durable runs.** Their brokers use
128 GiB Premium SSDs, which Azure rates as P10: 100 MB/s and 500 IOPS.
Durable periodic runs on the RF=1 cluster appended 209 to 219 MB/s across
three brokers, and `on_commit` runs 124 to 273 MB/s, while the same runs
received 1.8 to 2.0 GB/s of fire-and-forget traffic.[^p10] Unbatched durable
publishes on that cluster hit the commit timeout, with each fsync covering 1
to 3 records at 4 to 6 ms (#922). None of this is comparable to the NVMe
broker's numbers. For those rows, read the append rate, not ingress.

**Twelve keys reached only 8 of 12 shards.** The load generator named its keys
`k0` to `k11`. Under the stream's routing hash those land on shards
`[6,10,10,5,2,7,4,4,6,9,3,6]`: four shards idle and three doubled. On the
three-broker clusters this loads the brokers unevenly. #924 picks key names
that cover every shard evenly. The NVMe broker owns every shard itself, so
the skew does not change its placement.

**A 90-second run went on for 1178 seconds (#922).** The load generator
checked its deadline only between sends, and a fire-and-forget send could
wait behind a durable backlog. It also counted sends, not appends. #924 bounds
every send by the deadline and reports acked records separately. Broker-side
steady state was not affected.

**Publish connections can buffer far more than the ingress budget (#923).**
The client listener uses the cache transport's receive windows, 64 MiB per
stream and 256 MiB per connection, while the ingress budget is 16 MiB per
connection. On the RF=1 cluster, two brokers held 948 MB and 753 MB of unread
publishes and took about 20 minutes to drain. This is open.

**Tokens expired during long runs.** The control planes issue 8-hour tokens, and
runs longer than that failed with 401s. On one night an automatic OS upgrade
also restarted the in-memory control planes and lost their keys. Runs that
failed were rerun, and no partial run is quoted. #912 switched the harness to
refresh tokens, and #914 fixed the file mode of the rotated token.

**Deploy a build by its full commit SHA.** The first #927 A/B compared against
a build labelled "main" that was in fact an old install of the build before
#905. It showed delivery of 5.9 to 7.9 ms, which belonged to the old batcher,
not to #927's base. The deploy script also accepts only a full 40-character
SHA or a branch name. Every run now records the SHA it ran, and an A/B is
checked against it before it is read.

**Keep run names short.** The event-batch delay runs first failed because the
harness put every override in the run's directory name, which passed the
255-character limit for a file name. Overrides that only restate a default
are now left out of the name.

**Tokio timers round up to a millisecond.** A sub-millisecond delay built on
a `tokio` timer can cost a whole millisecond, and a zero delay still arms a
timer. That is how a 250 µs setting hid a 500 µs average wait (#926). A
latency path should not arm a timer it does not need.

**A subscription spending limit stops everything.** On 2026-10-02 at about
11:24 UTC the subscription's credit ran out. Azure disabled it and
deallocated every VM. The run chain kept going, and each later step failed
quickly in turn. All data up to 11:19 UTC was saved. One NATS tuning run lost
its series and is not used. The chain should stop at the first failure it
cannot explain.

## Reproducing

The charts on this page come from the raw runs:

```bash
pip install -r scripts/perf/requirements.txt   # matplotlib
python3 scripts/perf/v060_charts.py --sessions scripts/perf/azure/sessions
```

It writes the SVGs and `data.csv` to `docs-site/public/charts/perf-v060/`.
`data.csv` lists every plotted value with the run it came from. To get the
profile breakdown for one run:

```bash
python3 scripts/perf/azure/fold_categories.py \
  <cell>/felixperf-broker-0.folded.gz --cores <ss_cores> --mbs <ss_ingress_mb_s>
```

The NATS side ran from the harness in #916 (`scripts/perf/azure/nats/`):
`install.sh`, then `interleave.sh` for the pairs, `atomic-tune.sh` and
`fast-tune.sh` for the tuning, and `nats-latency.sh` for latency. Its README
lists every server setting and the exact `nats bench` command line of each
run, which each run also records in its `meta.env`.

## Appendix: runs behind each section

Each setup's runs are under `scripts/perf/azure/sessions/<results>/cells/`:

| Setup | Results directory |
|---|---|
| NVMe broker | `v060-a2-results` |
| RF=1 cluster | `v060-b2-results` |
| RF=3 cluster | `v060-c3-results` |

A run's directory name ends in its trial, `-t1` to `-t3`. The footnotes below
give the run names for every number on this page.

[^h-inmem]: NVMe broker, `best-inmem-l4-t1` to `-t3`.
[^h-dur]: NVMe broker, `best-dur-l1`, `best-dur-l2`, `best-dur-l4` and `best-dur-l8`, three trials each. fio from `v060-a2-results/system/felixperf-broker-0.fio.txt`.
[^h-256]: NVMe broker, in memory: `ab-felix-inmem-b64-p256-k48-t1`. Durable: `ab-felix-dur-b64-p256-k48-t1`, `ab-felix-dur-a64-p256-k48-t1` and `-t2`, which are the same Felix configuration.
[^h-lat]: NVMe broker, `ab927-pr927-lat-inmem-r1`, `-r2`, `ab927-pr927-lat-oncommit-r1`, `-r2` (directory names continue with their overrides).
[^nic]: `sessions/logs/experiments-a.log`.
[^mtu-check]: `v060-a2-results/system/nats/*.mtucheck.txt`.
[^fio]: `v060-a2-results/system/felixperf-broker-0.fio.txt`.
[^old-dur]: `l557-l4-io0-dur`, three trials.
[^prof-best]: `prof-best-l4`.
[^prof-steps]: `prof-l4-inmem` (before #905), `prof-801-l4` (#905), `prof-best-l4` (#905, MTU 3900, ACK threshold 64).
[^per-record]: `ab-felix-inmem-b64-p4096-k48-t1`, `ab-felix-inmem-b1-p4096-k48-t1`, `ab-felix-inmem-b64-p256-k48-t1`, `ab-felix-inmem-b1-p256-k48-t1`, `ab-felix-dur-b64-p4096-k48-t1`, `ab-felix-dur-b1-p4096-k48-t1`, `ab-felix-dur-b64-p256-k48-t1`, `ab-felix-dur-b1-p256-k48-t1`.
[^experiments]: In table order: `e1-base`, `e1-801`, `e2-ackelicit64`, `e7-801`, `e7-mimalloc`, `e8-mtu3900`, `best-inmem-l4`, three trials each.
[^one-listener]: `l557-l1-io0-inmem` (before #905) and `e8-mtu1500-l1` (#905).
[^batcher-before]: RF=1 cluster: `b375-inmem-lat-p0`, `b375-inmem-lat-p256`, `b375-inmem-lat-p4096`. RF=3 cluster: `c425-rf3-leader-commitack-lat-p0`. Three trials each.
[^mtu]: Four listeners: `e7-801`, `e8-mtu3900`. One listener: `e8-mtu1500-l1`, `e8-mtu3900-l1`. Profiles: `prof-801-l4`, `prof-best-l4`.
[^ack]: `e2-ackelicit64` against `e1-801` at MTU 1500; `best-inmem-l4` against `e8-mtu3900` at MTU 3900.
[^listeners]: `l557-l1-io0-inmem`, `l557-l2-io0-inmem`, `l557-l4-io0-inmem`; profile `prof-l1-inmem`.
[^l8]: `best-inmem-l8` and `best-inmem-l4`; profiles `prof-best-l8` and `prof-best-l4`.
[^io6]: `l557-l4-io6-inmem` against `l557-l4-io0-inmem`.
[^evdelay]: `lat-evdelay0-inmem`, `lat-evdelay0-oncommit`, `lat-evdelay50-inmem`, `lat-evdelay50-oncommit`, two trials each (directory names continue with their overrides).
[^lat-old]: `v060-a2-results/old-ab927-vs-167120f0/ab927-main-lat-inmem-r1` and `ab927-main-lat-oncommit-r1`. The harness labelled this build "main"; it was the build before #905.
[^lat-927]: `ab927-base-*` (before #927) and `ab927-pr927-*` (#927), runs `r1` and `r2` of `lat-inmem`, `lat-oncommit`, `fan4-b1` and `fan1-b64`. The throughput figures in the fanout runs are client-side, because the broker has no delivery counter.
[^lat-pre927]: `felix-lat-periodic-p256-t1`, `felix-lat-periodic-p4096-t1`, `felix-lat-oncommit-p4096-t1`, build `801fc22d`.
[^repl-lat]: RF=1 cluster: `b375-inmem-lat-p256`, `b375-periodic-lat-p256`, `b375-oncommit-lat-p256`. RF=3 cluster: `c425-rf3-leader-commitack-lat-p256`, `c425-rf3-leader-enqueueack-lat-p256`, `c425-rf3-quorum-lat-p256`, `c425-rf3-dquorum-periodic-lat-p256`, `c425-rf3-dquorum-oncommit-lat-p256`. Three trials each. The RF=1 periodic and `on_commit` runs forwarded all 22,050 publishes. The later RF=1 Leader and Quorum latency runs (`b425-rf1-*-lat-*`) were relayed too and are left out.
[^repl-tp]: `b425-rf1-leader-ingest-c24`, `b425-rf1-quorum-ingest-c24`, `c425-rf3-leader-commitack-ingest-c24`, `c425-rf3-leader-enqueueack-ingest-c24`, `c425-rf3-quorum-ingest-c24`, three trials each.
[^repl-dur]: `c425-rf3-dquorum-oncommit-ingest-c12-t1`, `-c24-t1`, `c425-rf3-dquorum-periodic-ingest-c12-t1`, `-c24-t1`. Append rate is the whole run's average from the before and after counters.
[^shapes]: RF=1 cluster: `b-shape-inmem-p{256,1024,4096}-b{1,64}-f64-t1`. RF=3 cluster: `c-shape-quorum-p{256,1024,4096}-b{1,64}-f64-t1`, without `p4096-b64`.
[^core-procs]: `ab-nats-ff-p256-s48-t1`. Its `meta.env` says `flag.gen_saturated=88.7`, which is `s.cpu_busy` from `felixperf-broker-0.after.txt`; the four generators' `after.txt` files read 47.2% to 52.1%. Calibration runs: `v060-a2-results/nats-calib/default_core_256_16_0_1_48_{1,2,4}_`, message counts from `m.publish_requests`.
[^tune]: `nats-fast-tune-{default,memory}-p{256,4096}-f64-w{16,256}-c16-s48-t1`, `nats-fast-tune-*-w64-c64-s48-t1`, `nats-async-tune-{default,memory}-p{256,4096}-w{1024,4000}-c16-s48-t1`. `nats-async-tune-memory-p4096-w4000` lost its series when the subscription stopped and is not used.
[^streams]: `nats-js-fast-always-p4096-f1-w{16,64,256,1024}-c16-s12-t1` and `-w{16,64,256}-c16-s24-t1`.
[^atomic-tune]: `nats-atomic-tune-p4096-a64-c{16,64,128}-s48-t1`: 98,579, 106,581 and 103,971 records/s.
[^pairs]: Felix runs `ab-felix-<pair>-k48-t<n>` and NATS runs `ab-nats-<pair>-s48-t<n>`, where `<pair>` is, in table order, `dur-a64-p4096` (trial 2), `dur-a64-p256` (trial 2), `dur-b1-p4096`, `dur-b1-p256`, `per-b64-p4096`, `per-b64-p256`, `inmem-b64-p4096`, `inmem-b64-p256`, `inmem-b1-p4096`, `inmem-b1-p256`, `ff-p4096`, `ff-p256`.
[^fast]: `ab-felix-dur-b64-p4096-k48-t1`, `ab-felix-dur-b64-p256-k48-t1`, `ab-nats-dur-b64-p4096-s48-t1`, `ab-nats-dur-b64-p256-s48-t1`.
[^lat-pairs]: `felix-lat-{oncommit,periodic}-p{256,4096}-t1` and `nats-lat-{oncommit,periodic}-p{256,4096}-t1`; the #927 rows are from the runs in the next footnote.
