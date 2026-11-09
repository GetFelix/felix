# Felix on local NVMe — consolidated findings (nvme1 + nvme2)

Two sessions on Azure L-series local NVMe (RAID0), eastus2, v0.3.1, real Entra,
TLS/QUIC, rf=1, OnCommit durable.

- **nvme1**: 3× L4as_v4 (4 vCPU, 2× NVMe RAID0 ~750 MB/s write), 12 shards.
- **nvme2**: 2× L8as_v4 (8 vCPU, 4× NVMe RAID0 ~1.5 GB/s write), 24 shards.

The point: on Premium SSD (~170 MB/s) durable ingest was disk-bound and the
"durable ~1 GB/s" claim was a page-cache burst. Local NVMe removes the disk as
the wall so the real bottleneck shows.

## 1. Durable = in-memory, SUSTAINED (nvme1, single loadgen, 4 KiB)

| Publishers | In-memory | Durable OnCommit |
|---|---|---|
| 1 | 328 | 295 |
| 3 | 877 | 870 |
| 6 | 1120 | 1061 |
| 12 | 1145 | 1128 |
| 24 | 1136 | **1151** |

256 B c24: in-memory 3.41 M msg/s, durable 3.56 M msg/s. Durable tracks in-memory
within ~5% and is indistinguishable at the plateau — **real and sustained, not a
page-cache burst**. Both plateau at ~1.15 GB/s = the single D4 loadgen's crypto
ceiling, not the brokers.

## 2. CPU-bound / commit-bound, NEVER disk-bound

Every broker CPU breakdown under load showed **iowait ~0-1.7%** on NVMe. The cost
is user + system + **softirq** (QUIC/UDP packet processing) — never disk. The
disk (0.75-1.5 GB/s write/broker) always had headroom.

## 3. Per-broker durable ceiling = the group-commit path, ~977 MB/s at ~48% CPU

> **Corrected 2026-09-20.** Both halves of the conclusion below are wrong. The
> section is left as it was written; the reasoning is part of the record.
>
> It is not the commit sequencer. `CommitSequencer` lives on `StreamState`,
> keyed per (stream, shard), so twelve shards is twelve independent commit
> paths and throughput did not move. Discarded in
> `docs/perf-investigation-sharding-ceiling.md` (#552). The ceiling is one
> `quinn` endpoint driver, a single task at ~88% of one core (#557).
>
> The number was the instrument. A `D4as_v5` generator tops out near
> 1,050 MB/s, so one generator could not tell the broker's limit from its own.
> With four generators the band is 842–926 MB/s, broker ≤69% busy, generators
> ≤31%.
>
> The prediction held anyway: throughput does scale with brokers. It scales
> because each broker has its own socket and its own endpoint driver, not
> because each has its own commit path. "More commit paths" would mean more
> shards help. They measure at 1.0x.

nvme2, all shards on one L8as_v4 broker (see #5), single loadgen direct:

| Concurrency | 1 | 4 | 8 | 16 | 24 | 32 |
|---|---|---|---|---|---|---|
| Durable MB/s | 308 | 839 | 949 | 950 | 934 | 910 |

Plateau **~950-977 MB/s** at **~48% CPU** (18us/20sy/9si), iowait 1.5%. Adding a
2nd/3rd loadgen does NOT go faster — they fail "publish queue full" (acks can't
drain; OnCommit waits on the commit sequencer). So one broker's durable OnCommit
is **commit-sequencer-bound, with ~half the CPU idle** — not CPU, not disk.
Implication: durable throughput scales with *brokers* (more commit paths), not
with per-broker cores. This is consistent with #1: on nvme1 each of 3 brokers did
~380 MB/s durable (well under 977), so no commit path saturated -> durable = in-memory.

## 4. Cross-broker forwarding roughly HALVES per-vCPU efficiency

- Direct to the shard owner (no forwarding): ~950 MB/s at ~48% of 8 vCPU ~= **~250 MB/s per busy vCPU**.
- Round-robin connections (⅔ of publishes forwarded owner-to-owner, double crypto):
  the multi-broker runs saturated brokers at **~140 MB/s per vCPU** (consistent with
  the Premium-SSD t1-a ~136).

A publish to a non-owner broker is decrypted, re-encrypted to the owner, and
decrypted again. Clients connecting to shard owners nearly double throughput-per-core.

## 5. Shard-assignment imbalance (a real finding)

Felix's control plane did NOT spread shards evenly: **48/0 on 2 brokers** (nvme2,
all on broker-0), **11/5/8 on 3 brokers** (nvme1). A reseed did not fix it. This
caps multi-broker aggregate throughput (idle brokers) and blocks a clean balanced
per-vCPU / cluster-ceiling measurement. Worth a control-plane investigation
(assignment + rebalance-on-registration).

## Net

On fast disk Felix's durable path is real and sustained, never disk-bound. The
limits are (a) the per-broker group-commit sequencer (~977 MB/s, CPU to spare),
(b) the QUIC crypto/packet tax (per-vCPU ~250 direct, ~140 with forwarding), and
(c) uneven shard assignment. The clean cluster ceiling / balanced per-vCPU needs
the shard-assignment issue fixed first.
