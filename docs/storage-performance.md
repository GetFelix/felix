# Durable Storage Performance

What durability costs, measured rather than argued, plus the regression budget
the durable log is held to.

Reproduce everything here with:

```sh
scripts/bench-durable-log.sh results.jsonl
```

## How the log is built for speed

Seven techniques, each taken from a log-structured engine that has already paid
for the lesson. They are listed in rough order of how much they matter.

### 1. Group commit, the single largest lever

An `fsync` flushes the *whole file*, not one caller's bytes. So when N appends
are in flight, one flush can satisfy all N. Every appender contends for the same
flush lock; the winner flushes, and the losers wake to find their target already
durable and return without flushing again.

Measured below: 253 durable appends/second at concurrency 1, 14,387 at
concurrency 64. Same code, same disk, **57× the throughput**, purely from not
making each append buy its own device flush.

This is the mechanism behind PostgreSQL's `commit_delay`, MySQL's binlog group
commit, and RocksDB's WAL group commit. `felix_storage_sync_batch_appends`
reports the fan-in actually achieved. A value near 1 under concurrent load means
appends are serialising on the device and something has regressed.

### 2. One `write` per batch, not per record

Records are encoded into a reusable staging buffer and handed to the kernel in a
single `write`. At 128-byte payloads the syscall, not the copy, is what costs,
which is why batching 16 records per append is worth 4.8× to 16× the throughput
of batching one, across every row below. The gain is largest under `on_commit`
(13–16×), where a batch amortises a device flush as well as a syscall, and
smallest for a single `none` publisher (4.8×), where there was little syscall
overhead to remove in the first place.

### 3. Writes and flushes are separate operations

A `write` lands in the page cache and is cheap; only `fsync` touches the device.
Keeping them apart is what makes group commit expressible at all, and it is why
`None` and `Periodic` can acknowledge in ~1µs.

### 4. Positioned reads (`pread`)

A `seek` + `read` pair mutates the file cursor, so a shared descriptor cannot
serve two readers at once and every read costs an extra syscall. `pread` takes
the offset as an argument: one descriptor per segment serves any number of
concurrent readers with no lock. Kafka, RocksDB and LevelDB all do this on their
immutable files.

### 5. Preallocation

Appending past end of file makes the filesystem allocate blocks and update the
inode's block map on the write path, and invites fragmentation as segments from
different shards interleave. `fallocate(FALLOC_FL_KEEP_SIZE)` on Linux and
`F_PREALLOCATE` on macOS reserve the blocks up front without changing the file's
logical length, so recovery's "valid bytes end at EOF" reasoning still holds.

### 6. Sparse indexes

One entry per `index_spacing_bytes`. A read binary-searches the entries, then
scans forward at most one interval. Locating an offset is `O(log n)` plus a
bounded read, instead of a scan from the head of the log.

### 7. `fdatasync` over `fsync`

An append changes file data and size, not the owner, mode or times a full `fsync`
also flushes. On macOS this is `F_FULLFSYNC` instead, because POSIX `fsync` there leaves
data in the drive's volatile cache, so anything weaker would be measuring a
promise the platform does not keep.

### 8. Releasing a commit turn wakes one publisher, not all of them

Group commit is what makes many publishers cheap; the commit sequencer is what
keeps them in order afterwards (see [durable-storage.md](durable-storage.md)).
Handing the turn on is where that ordering can quietly undo the first technique.

Waking everyone parked behind a turn makes each of them acquire the lock, find
it is still not their turn, and park again. Only one can proceed, so with N
publishers in flight a single commit costs N wake-ups to accomplish one, and
the work per commit grows with load. That is the classic thundering herd, and it
turns concurrency from a throughput lever into a throughput tax.

Waking the single waiter whose turn it now is costs one wake-up per commit at
any N. The registry is keyed by offset, so finding that waiter is a lookup, not
a scan.

End-to-end durable publishes per second, same machine and harness, varying only
the wake-up:

| Publishers | Wake all | Wake one | Gain |
| ---: | ---: | ---: | ---: |
| 1 | 662,086 | 664,694 | 1.00× |
| 2 | 405,403 | 417,018 | 1.03× |
| 4 | 255,725 | 474,678 | 1.86× |
| 8 | 222,222 | 481,490 | 2.17× |
| 16 | 156,565 | 476,683 | 3.04× |
| 32 | 82,005 | 475,677 | 5.80× |
| 64 | 40,320 | 465,560 | **11.5×** |

Read the shapes, not the absolute numbers: an M4 Max laptop, in-process, no QUIC,
`FsyncMode::None` so no device flush hides the effect. Waking all of them decays
without a floor: at 64 publishers it retains 6% of its single-publisher rate,
so adding publishers makes the broker slower in absolute terms. Waking one holds
flat at ~0.72× from four publishers on.

The step from one publisher to two costs ~38% in **both** columns. That is not
the herd. It is the price of parking and waking at all, which an uncontended
publisher never pays. Everything past two is the herd.

### 9. Each log flushes on its own thread

An `fsync` blocks, so it cannot run on a reactor thread. Handing it to
`spawn_blocking` keeps the reactor free but puts the flush in Tokio's one
blocking-pool queue, shared with reads, rollovers and every other shard's
flushes, and that hand-off gets slower the more shards flush at once.

Each log has a thread that runs only its flushes, so a flush is one channel send
and one wake-up. The thread starts on the log's first flush and exits after 10 s
without one, so quiet shards do not hold threads.

Round trip for a no-op flush, i.e. the dispatch alone (`dispatch_overhead` in
`crates/server/felix-storage/src/io/log_thread/tests.rs`, release build, 4 runtime
workers, one task per log), mean / p99 in µs:

| Logs flushing at once | macOS `spawn_blocking` | macOS flush thread | Linux `spawn_blocking` | Linux flush thread |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 7.5 / 16 | 7.3 / 15 | 38 / 166 | 20 / 46 |
| 8 | 53 / 218 | 17 / 43 | 99 / 1,066 | 36 / 106 |
| 32 | 223 / 734 | 46 / 111 | 407 / 4,579 | 124 / 248 |

macOS is an M4 Max laptop; Linux is a Docker Desktop VM on the same machine
(16 vCPUs), so read the ratios rather than the absolute numbers. On NVMe, where
a `fdatasync` can be tens of microseconds, the pool's hand-off under
contention was larger than the sync it protected.

`FELIX_STORAGE_IO_URING=1` submits flushes to one process-wide ring instead.
The kernel still runs each `fsync` on a worker thread of its own, and in the
same VM a single log's real `fdatasync` round trip measured ~47 µs through the
ring against ~24 µs on a flush thread, so it stays opt-in. The ring's thread
waits on completions and on an eventfd that callers signal, so a flush queued
while other logs' syncs are in flight reaches the kernel at once instead of
waiting for one of them to finish.

### 10. Compaction runs beside writes, not in front of them

Cache and counter compaction used to run inline: the write that pushed a log
over its threshold rewrote the whole live set under the shard lock before it
returned, and every write queued behind that lock waited too. Compaction now
runs on a background task (see `docs/durable-storage.md`, "Cache and counter
compaction"). A write waits for at most one batch of copies being staged, which
is a buffered write of 64 records with no flush, and the pass paces itself
against `FELIX_STORAGE_COMPACTION_BYTES_PER_SEC`.

An indicative before/after, on a shared and loaded laptop (load average about
20 on 16 cores), so read the shape rather than the digits. One writer put 20,000
keys of 1 KiB six times over (120,000 puts), with `FsyncMode::None` and 16 MiB
segments, so the cache compacted mid-run with about 20 MiB live:

| | total | p50 | p99 | p99.99 | max |
| --- | --- | --- | --- | --- | --- |
| inline (before), 2 runs | 19.7–22.5 s | 4.8–5.7 µs | 16–19 µs | 2.1–5.9 ms | 18.6–21.0 s |
| background (after), 6 runs | 1.0–1.8 s (one run 9.5 s) | 4.5–5.8 µs | 12–27 µs (one run 3.1 ms) | 2.8–22 ms | 33–899 ms |

Before, the one put that triggered compaction waited the entire rewrite, about
20 seconds. After, no put waited for compaction; the remaining tail tracks
segment rolls and machine load, and one run in six was slow throughout.

### 11. Appends write on the log's own thread

A `write` into the page cache blocks once the kernel throttles a process that
dirties pages faster than the device takes them, so each log's appends run on
a thread of their own, with the segment lock released during the `write` (see
`docs/durable-storage.md`, "Where the append runs").

`felix-log-tool bench`, 128-byte payloads, records/s and p50/p99 append
latency in µs, three runs each, on an M4 Max with the log on an external SSD.
Before is the `write` on the caller's reactor thread:

| fsync | batch | publishers | before rec/s | after rec/s | before p50/p99 | after p50/p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| none | 1 | 1 | 223k–264k | 91k–96k | 3 / 6–8 | 9–10 / 15–16 |
| none | 1 | 8 | 96k–106k | 209k–222k | 40–47 / 397–423 | 35–37 / 49–50 |
| none | 16 | 1 | 1.68M–1.90M | 701k–951k | 6–7 / 9 | 13–18 / 19–27 |
| none | 16 | 8 | 1.14M–1.52M | 1.73M–2.25M | 10–37 / 1,223–3,046 | 52–68 / 79–101 |
| on_commit | 1 | 8 | 1.3k | 1.6k–2.3k | 5,647–5,945 / 12,194–12,251 | 3,488–5,348 / 6,904–7,754 |
| on_commit | 16 | 8 | 19k–21k | 22k–23k | 5,367–6,010 / 11,972–12,484 | 5,394–5,543 / 7,802–11,923 |

A lone publisher pays a thread wake-up on each side of every append, about
10 µs, which is more than the `write` it moves: it loses about 60% of its
throughput, less once its batches are large. Several publishers on one log
gain, because they queue on the thread instead of on the segment lock across
each other's `write`, which is where the 400 µs and multi-millisecond tails
came from. Under `on_commit` a flush now covers every append queued when it
starts: with 16 publishers of 4 KiB records, 15–16 appends per flush against
12–13 before, counted from `DiskLog::flushes`.

## Where the time goes

```mermaid
graph LR
    A["append() called"] --> B["encode batch<br/><i>~0.1µs, no allocation</i>"]
    B --> C["one write() syscall<br/><i>~1µs to page cache</i>"]
    C --> D{"fsync policy"}
    D -->|"None / Periodic"| E["return<br/><b>~1µs total</b>"]
    D -->|"OnCommit"| F["wait for a flush<br/><i>shared with concurrent appends</i>"]
    F --> G["device flush<br/><b>~4ms on APFS</b>"]
    G --> H["return<br/><b>~4ms total</b>"]

    classDef fast fill:#e9f5ec,stroke:#4a8a5e,color:#16301f
    classDef slow fill:#fdf0e3,stroke:#b07d3a,color:#3d2a12
    class A,B,C,E fast
    class F,G,H slow
```

Roughly four thousand times separates "in the page cache" from "on the device":
about 1µs against about 4ms. Nothing in the code can close that gap; group
commit exists to *amortise* it.

## Measured results

**Hardware and configuration.** Apple Mac Studio (`Mac16,9`), Apple M4 Max,
16 CPUs, macOS 26.6.1 (Darwin 25.6.0), APFS on internal NVMe. 128-byte payloads,
8,000 measured records per run after a 2,000-record warm-up, 64 MiB segments,
4 KiB index spacing, preallocation on. `felix-log-tool bench`, release profile.
Latencies are per `append` call, so at batch 16 one sample covers 16 records.

| Policy | Batch | Concurrency | Records/s | p50 | p99 | p999 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `none` | 1 | 1 | 571,898 | 1µs | 2µs | 5µs |
| `none` | 1 | 8 | 252,825 | 3µs | 391µs | 730µs |
| `none` | 1 | 64 | 255,997 | 3µs | 787µs | 1.5ms |
| `none` | 16 | 1 | 2,768,326 | 3µs | 4µs | 8µs |
| `none` | 16 | 8 | 2,121,453 | 51µs | 337µs | 738µs |
| `none` | 16 | 64 | 1,843,566 | 5µs | 1.7ms | 2.2ms |
| `periodic` (250ms) | 1 | 1 | 492,106 | 1µs | 3µs | 7µs |
| `periodic` | 1 | 8 | 272,182 | 3µs | 413µs | 927µs |
| `periodic` | 1 | 64 | 267,180 | 3µs | 955µs | 2.6ms |
| `periodic` | 16 | 1 | 2,671,267 | 3µs | 5µs | 8µs |
| `periodic` | 16 | 8 | 1,990,194 | 57µs | 261µs | 690µs |
| `periodic` | 16 | 64 | 1,854,732 | 129µs | 255µs | 284µs |
| `on_commit` | 1 | 1 | 253 | 3.99ms | 6.0ms | 10.7ms |
| `on_commit` | 1 | 8 | 2,006 | 3.99ms | 6.9ms | 8.3ms |
| `on_commit` | 1 | 64 | 14,387 | 4.07ms | 8.6ms | 9.2ms |
| `on_commit` | 16 | 1 | 4,038 | 3.96ms | 6.5ms | 9.7ms |
| `on_commit` | 16 | 8 | 28,670 | 4.00ms | 9.8ms | 12.1ms |
| `on_commit` | 16 | 64 | 185,905 | 4.96ms | 8.1ms | 9.2ms |

**Recovery is fast enough not to shape restart planning.** A 1.01 GiB active
segment - 7,000,000 records - is fully validated in 0.32s, about 3.2 GiB/s.
Since only the active segment is scanned in full, restart cost is bounded by
`segment_size_bytes` rather than by total data on disk: at the 256 MiB default,
that is under a tenth of a second.

### What this says

**Durability is free until you ask for the device.** `none` and `periodic` are
within noise of each other, because the periodic flush runs off the append path, so its
cost does not appear in append latency at all. A durable stream on `periodic`
costs roughly what a non-durable one costs, plus the storage write itself.

**`OnCommit` latency is a hardware constant.** p50 sits at ~4ms in every single
row, from concurrency 1 to 64 and from batch 1 to 16. That is one APFS device
flush. No amount of software makes a single durable commit faster than the
device; the only variable is how many commits share one.

**Throughput under `OnCommit` scales almost linearly with concurrency.**
253 → 2,006 → 14,387 records/second at concurrency 1 → 8 → 64. Combined with
batching, 185,905 records/second, over 700× the single-publisher number, on the
same disk, with the same guarantee.

**Tail latency degrades with concurrency for the cheap policies.** `none` at
concurrency 64 shows p999 of 1.5ms against 5µs at concurrency 1.

The likely cause is contention on the segment write lock rather than I/O
(appends must assign offsets in order, and `none` does no device work to hide
behind), but that is an inference from the shape of the numbers, not something
measured. Confirming it needs lock-wait instrumentation on the append path,
which does not exist yet. It matters for high-fanout publishers on non-durable
streams and is the most likely place a future optimisation pays off.

## Regression budget

CI does not yet gate on these. They are the numbers a change should be measured
against, and a breach is a design conversation rather than an automatic failure.

| Guard | Budget | Rationale |
| --- | --- | --- |
| `none` / `periodic` p999, batch 1, concurrency 1 | ≤ 25µs | 3–5× the measured 5–7µs. Anything worse means the non-durable path picked up real work |
| `none` / `periodic` throughput, batch 1, concurrency 1 | ≥ 300k records/s | ~60% of measured. Guards against a per-record syscall or allocation creeping in |
| `on_commit` p50 | ≤ 1.5× one device flush | The floor is hardware. Exceeding it means an append is buying more than one flush |
| `on_commit` throughput at concurrency 64 | ≥ 40× the concurrency-1 figure | Group commit is the design. Falling toward 1× means the flush lock has stopped batching |
| `felix_storage_sync_batch_appends` under concurrent load | ≥ 8 | Direct measurement of the above; alertable in production |
| Batch-16 throughput vs batch-1 | ≥ 4× | Confirms one `write` per batch, not per record |
| Recovery time | ≤ 1s per GiB of active segment | Measured at 0.32s for a 1.01 GiB segment (~3.2 GiB/s), so the budget carries about 3× headroom. Only the active segment is fully scanned |

### Segment size is a latency knob, not only a recovery knob

`segment_size_bytes` is usually presented as a recovery-time dial: only the
active segment is scanned in full at startup, so a smaller segment restarts
faster. That is true, and it is not the whole trade.

Every rollover costs several device flushes (sealing the retired segment, and
creating and directory-syncing its replacement), and those flushes land on
whichever appends are in flight. Smaller segments roll more often, so the cost
appears more often, and it appears in the tail:

| Segment size | p999, 16 publishers | Rollovers per 20k records |
| --- | --- | --- |
| 256 MiB (default) | ~0.9ms | 0 |
| 16 MiB | ~1.3ms | ~1 |
| 1 MiB | ~30ms | ~20 |
| 128 KiB | ~112ms | ~160 |

Trading a 256 MiB segment for a 1 MiB one buys about 0.25s of restart time and
costs roughly 30× on p999. Below a few MiB the log is rolling often enough that
tail latency is dominated by rollover flushes rather than by appends. Prefer the
default unless restart time is genuinely the binding constraint, and if it is,
measure the tail rather than assuming recovery is the only thing that moved.

#### What a rollover no longer costs

Those numbers are the flushes themselves, and they are irreducible. Two things
that used to ride along with them are not:

* **A rollover no longer stalls the runtime.** The segment set is behind a
  synchronous lock, so a publisher queued on it parked a Tokio worker rather
  than yielding it. Held across a rollover's two device flushes, that stalled
  everything else on the runtime (other shards, subscriber fanout, the accept
  loop) once as many publishers were queued as there were workers. Appends now
  wait on an async gate and yield.
* **Rollover contention no longer rejects appends.** An append that lost the
  race for the new segment `MAX_ROLL_ATTEMPTS` times returned `Unsupported`.
  With 128 KiB segments, 16 publishers on one shard and 4 workers, that was
  6 runs in 20. It is 0 in 20 now: the gate admits one rollover at a time, and
  the publisher that rolls appends under the same gate rather than re-entering
  the race it just won.

Neither makes a rollover cheaper, so the table above still stands.

### Rolling segments in the background does not help

The obvious fix for the table above is to stop doing rollover work on the append
path: build the replacement segment ahead of time, off the lock, and swap it in
with a pointer write. `rollover_threshold_percent` implements exactly that. It
is disabled by default, because it was measured and it is worse.

Five 200k-record runs, 16 MiB segments, 16 publishers, median across runs:

| | p99 | p999 | max | throughput |
| --- | --- | --- | --- | --- |
| Inline roll (default) | 669µs | **3.98ms** | 47.0ms | 99.1k rec/s |
| Background roll | 667µs | **7.97ms** | 50.8ms | 98.0k rec/s |

The upper tail is where it shows: the worst background-roll run reached 19.0ms
p999 against 5.0ms for the inline path. At 256 MiB and at 1 MiB the two were
within noise. The background roll was not faster in any configuration tested.

The reason is `F_FULLFSYNC`. Durability on macOS means flushing the device write
cache, and that flush does not overlap with concurrent writes, so moving a
rollover's flushes off the append lock does not hide them, it only stops
confining them. Instead of a queue behind one lock, the same cost lands on
whichever appends happen to be in flight, which is strictly worse for the tail.

Two things are worth keeping from the exercise. The first is that reasoning is
specific to a flush that reaches the device; where `fdatasync` returns once the
write is in the kernel, overlap is real and the trade may invert, which is why
the code is kept and configurable rather than deleted. The second is what the
work actually did buy: measuring it exposed an `fsync` of a freshly created
(and therefore empty) index file sitting inside the rollover path. Removing it
improved the **default** inline path from 5.12ms to 3.98ms median p999 (-22%)
and 61.8ms to 47.0ms max (-24%).

### Operating envelope

| If you need | Choose | Expect |
| --- | --- | --- |
| Lowest latency, loss acceptable on machine failure | `none` | ~1µs p50, ~500k records/s single publisher |
| A bounded loss window, latency unchanged | `periodic { 250ms }` | ~1µs p50; up to 250ms of writes at risk |
| No loss of an acknowledged record | `on_commit` | ~4ms p50 per publisher; scale with concurrency and batching |

`periodic` is the default because it is the only one of the three whose cost is
invisible on the append path while still bounding loss.

## Reading this before M5

Replication compounds the flush cost: a quorum write is at minimum one local
flush plus a network round trip to a follower that is also flushing. Two
consequences follow from the numbers above and should shape M5's design rather
than be discovered during it:

1. **Per-record replication is not viable at `on_commit`.** At 253 durable
   appends/second per publisher, a design that flushes once per replicated record
   caps a node in the low hundreds of writes/second. Replication must batch, and
   the batch must span publishers, the same argument that produced group commit
   locally.
2. **The follower's flush should overlap the leader's, not follow it.** Serialising
   them doubles a 4ms constant. Group commit already proves the machinery for
   fanning one flush out to many waiters; the replication path should reuse it
   rather than invent a second one.

## Methodology notes

- **Warm-up is mandatory.** The first appends pay for segment creation,
  preallocation and page-cache faults. `--warmup-records` defaults to 2,000 and
  those samples are discarded.
- **Each run gets a fresh directory.** Reusing one lets an earlier run's segments
  and page-cache residency skew the next.
- **Throughput is measured after a final flush.** A policy that leaves data in
  the page cache has not finished its work when the last append returns.
- **Percentiles are nearest-rank** over every `append` call, not interpolated.
- **The environment is recorded in the output.** A latency figure without the
  filesystem and device it came from is not reproducible, and that is the most
  common way benchmark results become unusable six months later.
- **These are storage-layer numbers**, measured against `DiskLog` directly. They
  exclude QUIC, framing and fanout. The end-to-end broker figures come from
  `latency-demo` and are a separate measurement.
