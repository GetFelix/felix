# The metadata Raft benchmark

This page says how to measure the control plane's Raft group, and records the
numbers for today's store (redb, openraft 0.9). The store change in
[metadata-raft-design.md](metadata-raft-design.md#moving-to-the-segment-store) is held to
"no cell worse than these".

## What it measures

The benchmark runs real control-plane members in one process. Each has its own
redb directory and peer listener, and is driven through the store traits the
HTTP API uses. A write sent to a follower is forwarded to the leader, as it is
behind a load balancer. The code is
`services/felix-controlplane-service/tests/metadata_bench.rs`.

| Cell | What it does | Metrics |
|---|---|---|
| `propose/m<1,3>/<256B,16KiB>/p<1,16,128>` | Closed-loop namespace creates with that payload and that many concurrent proposers | `ops_per_s`, `p50_ms`, `p99_ms`, `bytes_written_per_entry` (Linux) |
| `stream_create` | One stream create after another, three members | the same, without bytes |
| `placement_update` | Shard assignments rewritten by four proposers, three members | the same |
| `replica_report` | Replica reports for every shard of one stream from 16 proposers, the steady write load of the log | the same |
| `failover` | The leader is stopped; the time until a write through a survivor is acknowledged | `p50_ms`, `max_ms`, `rounds` (acknowledged), `failed_rounds` |
| `snapshot_build/<1MiB,32MiB>` | A snapshot of that much metadata, with a writer running | `build_ms` (the state machine's snapshot call), `longest_write_ms` |
| `restart_to_ready/<size>` | Stop a member behind that snapshot and start it again; time until it confirms leadership | `ready_ms` |
| `snapshot_install/<size>` | Add an empty learner to a one-member group whose log is gone behind that snapshot; the time until the learner has restored the snapshot | `install_ms`, `ok` (the learner restored) |
| `idle/m3` | Three idle members, whole process; runs first so the memory is the process plus the members | `cpu_percent`, `process_rss_mib` (Linux) |

Not measured yet: fsyncs per committed entry (the bytes written per entry stand
in), and the time to store a snapshot after it is built, because the Raft handle
does not report when a snapshot has landed.

## How to run it

On a laptop, for a quick look. The numbers mean little there:

```bash
scripts/ci/metadata_bench.sh bench.json quick
python3 scripts/ci/metadata_bench.py table bench.json
```

For a number worth recording, use Actions: **Metadata Raft benchmark**
(`metadata-bench.yml`), "Run workflow", scale `full`. Leave `base` empty to
get one table. To gate a change, set `base` to the commit before it. The job
builds both commits and runs them alternately three times each (`reps`), so
drift on the runner lands on both sides. Each load cell throws away a warmup
before its measured window. Per metric the median of the runs is compared; a
cell's noise band is the spread of the base runs, (max - min) / median, never
under 10%. Head fails a cell when it is worse than the base median by more than
the band. A cell head does not report fails. Throughput, `ok` and `rounds` are
better higher; every other metric is better lower, so a failed round or a
snapshot install that did not complete fails the cell. Both sides run the head
commit's benchmark file, so a base that predates it still works as long as the
store traits it uses have not changed.

## The redb baseline

Median of three `full` runs on the shared self-hosted runner (the head side of
an A/A run of this commit against main, Actions run 43148436768). Throughput
and latency on that runner are noisy; the gate holds a change to the noise band
of its own base runs, not to these digits.

| Cell and metric | Value |
|---|---|
| failover failed_rounds | 0 |
| failover max_ms | 2268 |
| failover p50_ms | 2014 |
| failover rounds | 5 |
| idle/m3 cpu_percent | 0.8 |
| idle/m3 process_rss_mib | 39.78 |
| placement_update failed | 0 |
| placement_update ops_per_s | 1614 |
| placement_update p50_ms | 2.319 |
| placement_update p99_ms | 6.132 |
| propose/m1/16KiB/p1 bytes_written_per_entry | 1.851e+05 |
| propose/m1/16KiB/p1 failed | 0 |
| propose/m1/16KiB/p1 ops_per_s | 745.7 |
| propose/m1/16KiB/p1 p50_ms | 1.267 |
| propose/m1/16KiB/p1 p99_ms | 2.659 |
| propose/m1/16KiB/p128 bytes_written_per_entry | 1.326e+05 |
| propose/m1/16KiB/p128 failed | 0 |
| propose/m1/16KiB/p128 ops_per_s | 2350 |
| propose/m1/16KiB/p128 p50_ms | 53.09 |
| propose/m1/16KiB/p128 p99_ms | 102.7 |
| propose/m1/16KiB/p16 bytes_written_per_entry | 1.395e+05 |
| propose/m1/16KiB/p16 failed | 0 |
| propose/m1/16KiB/p16 ops_per_s | 1733 |
| propose/m1/16KiB/p16 p50_ms | 8.896 |
| propose/m1/16KiB/p16 p99_ms | 12.86 |
| propose/m1/256B/p1 bytes_written_per_entry | 5.835e+04 |
| propose/m1/256B/p1 failed | 0 |
| propose/m1/256B/p1 ops_per_s | 1575 |
| propose/m1/256B/p1 p50_ms | 0.5401 |
| propose/m1/256B/p1 p99_ms | 1.256 |
| propose/m1/256B/p128 bytes_written_per_entry | 3586 |
| propose/m1/256B/p128 failed | 0 |
| propose/m1/256B/p128 ops_per_s | 2.487e+04 |
| propose/m1/256B/p128 p50_ms | 5.01 |
| propose/m1/256B/p128 p99_ms | 6.989 |
| propose/m1/256B/p16 bytes_written_per_entry | 9734 |
| propose/m1/256B/p16 failed | 0 |
| propose/m1/256B/p16 ops_per_s | 9097 |
| propose/m1/256B/p16 p50_ms | 1.645 |
| propose/m1/256B/p16 p99_ms | 3.624 |
| propose/m3/16KiB/p1 bytes_written_per_entry | 5.532e+05 |
| propose/m3/16KiB/p1 failed | 0 |
| propose/m3/16KiB/p1 ops_per_s | 243.6 |
| propose/m3/16KiB/p1 p50_ms | 3.842 |
| propose/m3/16KiB/p1 p99_ms | 11.21 |
| propose/m3/16KiB/p128 bytes_written_per_entry | 4.014e+05 |
| propose/m3/16KiB/p128 failed | 0 |
| propose/m3/16KiB/p128 ops_per_s | 562.5 |
| propose/m3/16KiB/p128 p50_ms | 220.5 |
| propose/m3/16KiB/p128 p99_ms | 325.8 |
| propose/m3/16KiB/p16 bytes_written_per_entry | 4.056e+05 |
| propose/m3/16KiB/p16 failed | 0 |
| propose/m3/16KiB/p16 ops_per_s | 466.2 |
| propose/m3/16KiB/p16 p50_ms | 31.53 |
| propose/m3/16KiB/p16 p99_ms | 110.9 |
| propose/m3/256B/p1 bytes_written_per_entry | 1.721e+05 |
| propose/m3/256B/p1 failed | 0 |
| propose/m3/256B/p1 ops_per_s | 540.2 |
| propose/m3/256B/p1 p50_ms | 1.472 |
| propose/m3/256B/p1 p99_ms | 8.309 |
| propose/m3/256B/p128 bytes_written_per_entry | 8569 |
| propose/m3/256B/p128 failed | 0 |
| propose/m3/256B/p128 ops_per_s | 8001 |
| propose/m3/256B/p128 p50_ms | 14.96 |
| propose/m3/256B/p128 p99_ms | 58.86 |
| propose/m3/256B/p16 bytes_written_per_entry | 1.996e+04 |
| propose/m3/256B/p16 failed | 0 |
| propose/m3/256B/p16 ops_per_s | 3912 |
| propose/m3/256B/p16 p50_ms | 3.836 |
| propose/m3/256B/p16 p99_ms | 6.701 |
| replica_report failed | 0 |
| replica_report ops_per_s | 3912 |
| replica_report p50_ms | 3.976 |
| replica_report p99_ms | 6.027 |
| restart_to_ready/1MiB ready_ms | 381.4 |
| restart_to_ready/32MiB ready_ms | 2.019e+04 |
| snapshot_build/1MiB build_ms | 6.94 |
| snapshot_build/1MiB longest_write_ms | 71.27 |
| snapshot_build/32MiB build_ms | 304.8 |
| snapshot_build/32MiB longest_write_ms | 1e+04 |
| snapshot_install/1MiB install_ms | 455.2 |
| snapshot_install/1MiB ok | 1 |
| snapshot_install/32MiB install_ms | 4.713e+04 |
| snapshot_install/32MiB ok | 1 |
| stream_create failed | 0 |
| stream_create ops_per_s | 526.6 |
| stream_create p50_ms | 1.567 |
| stream_create p99_ms | 7.204 |
