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
| `failover` | The leader is stopped; the time until a write through a survivor is acknowledged | `p50_ms`, `max_ms` |
| `snapshot_build/<1MiB,32MiB>` | A snapshot of that much metadata, with a writer running | `build_ms` (the state machine's snapshot call), `longest_write_ms` |
| `restart_to_ready/<size>` | Stop a member behind that snapshot and start it again; time until it confirms leadership | `ready_ms` |
| `snapshot_install/<size>` | Add an empty member to a group whose log is gone behind that snapshot | `install_ms` |
| `idle/m3` | Three idle members, whole process | `cpu_percent`, `rss_mib` (Linux) |

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
builds both commits, runs base, head, then base again, and compares. The second
base run is the A/A run: the gap between the two base runs of a cell is that
cell's noise band, and head fails a cell only when it is worse than base by
more than the band (at least 5%). A cell head does not report fails. Throughput
is better higher; every other metric is better lower. Both sides run the
head commit's benchmark file, so a base that predates it still works as long as
the store traits it uses have not changed.

## The redb baseline

Not yet recorded: the table below is filled from the first `full` run on the
runner.

| Cell and metric | Value |
|---|---|
