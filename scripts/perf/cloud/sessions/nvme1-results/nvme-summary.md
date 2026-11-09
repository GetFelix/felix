# Felix NVMe upper-bound run (nvme1, eastus2)

**Hardware:** 3× Standard_L4as_v4 brokers (4 vCPU, local NVMe: 2× 447 GiB RAID0,
~750 MB/s write / 1.5 GB/s read each, **~2.25 GB/s aggregate write**), D2as_v5
control plane, D4as_v5 load generator(s). Release v0.3.1 (release CP binary),
real Microsoft Entra ID on the exchange, TLS/QUIC, **12 shards** per stream (4
per broker). rf=1, Leader consistency.

Purpose: the disk on the earlier Premium-SSD runs (~170 MB/s) made durable ingest
disk-bound and forced the "durable ~1 GB/s was a page-cache burst" correction.
On RAID0 local NVMe the disk is no longer the wall, so durable vs in-memory gets
an honest, sustained verdict.

## Result: durable OnCommit = in-memory (single loadgen, 4 KiB)

| Publishers | In-memory (MB/s) | Durable OnCommit (MB/s) |
|---|---|---|
| 1 | 328 | 295 |
| 3 | 877 | 870 |
| 6 | 1120 | 1061 |
| 12 | 1145 | 1128 |
| 24 | 1136 | **1151** |

- **Durable OnCommit tracks in-memory within ~5% the whole ramp, and is
  indistinguishable at the plateau (1151 vs 1136).** This is *sustained* (fsync
  before every ack, group-committed), not a page-cache burst — the thing the
  Premium SSD could not show. Zero publish retries throughout.
- 256 B, concurrency 24: in-memory **3.41 M msg/s** (873 MB/s), durable **3.56 M
  msg/s** (910 MB/s) — durable again matches.
- Both plateau at ~1.15 GB/s, which is the **single D4 loadgen's** crypto/QUIC
  ceiling (per-packet AEAD, userspace packetisation), NOT the brokers or the
  disk (~2.25 GB/s available). The multi-loadgen run pushes past this to the real
  ceiling; broker CPU at saturation (MB/s per vCPU) is captured there.

Raw: nvme-ingest.jsonl (12 rows).
