# Plan: group-commit the cache write path

Status: **implemented.** The design below shipped as written: the sequencer
was lifted into `felix-storage` (`commit_order.rs`, shared with the broker's
publish path), `put_checked`/`delete_checked` follow the stage → commit →
turn-wait → apply flow, compaction is gated on the apply step with nothing
staged behind it, and the tests in `crates/server/felix-storage/src/cache/log_cache/tests/`
cover the throughput regression (flush-count, revert-verified), ordering,
durability-before-visibility, cancellation, and compaction under load. The
crash-mid-window test is deferred to the `felix-log-tool` kill harness; the
on-disk format and recovery path are untouched by this change.

## Problem

Concurrent cache writes do not scale. On the Azure NVMe run, 8 concurrent
writers to one cache shard got the **same ~230 puts/s as a single writer**, just
with ~8× the per-put latency (~4 ms → ~33 ms p50 at concurrency 8). The durable
*stream append* path, by contrast, amortises fsync under concurrency to >1 GB/s.
Same disk, same fsync. The difference is entirely in how the two paths commit.

This is the `OnCommit` cache-write counter-example already flagged in
`docs-site/.../real-network-performance.md` ("the durable *cache write* path
serialises on it"). It is a throughput bug, not a durability bug: the writes are
correct, they just cannot batch.

## Root cause

`crates/server/felix-storage/src/cache/log_cache.rs`:

- Each `CacheShard` has a single `state: Mutex<ShardState>` (`{ log, index }`).
- `put_checked` / `delete_checked` take that mutex and, **while holding it**, call
  `write()`, which calls the *atomic* `DiskLog::append()`, which does append **and fsync**
  in one call.
- So every write to a shard is fully serial: each holds the lock across a whole
  device flush. Throughput is `1 / fsync_time` (~230/s on a 3.6 ms Premium SSD),
  regardless of how many writers are waiting. The lock is also held across
  `ensure_index`, the observer `notify`, and compaction.

Because the fsyncs happen one-at-a-time under the lock, the DiskLog's group
commit (`disk_log/sync.rs`: "one fsync serves many waiters") never has more than
one append in flight to batch.

### Why the stream path does not have this

`crates/server/felix-broker/src/broker/publish.rs` (publish path) splits the write in two and
lets many run concurrently:

1. `durable.begin_append(payloads)` → `PendingAppend`: claims disk offsets under
   the short segment lock, **no fsync**.
2. `commit_sequencer.reserve(first, last+1)` → a `turn`: claims the commit order
   *immediately*, before the durability wait.
3. `durable.commit(&pending)`: waits for the fsync, which is **group-committed**
   across every concurrent `commit` in the window (`DiskLog::append_pending` +
   `commit`, `disk_log.rs`).
4. `turn.wait()`: blocks until every lower offset has been applied.
5. Apply to the in-memory replay ring + fanout, **in disk-offset order**.

The `CommitSequencer` (`crates/server/felix-storage/src/commit_order.rs`) is the crux:
commits complete out of order, but step 4/5 re-serialise the *observable* effects
into disk order, so "what the log says" and "what readers/subscribers see" never
disagree, even though the expensive fsyncs ran in parallel.

The cache has the same two halves (durable append, then index + observer update)
but runs them under one lock with no sequencer, so it cannot let the fsyncs
overlap.

## Invariants the fix must preserve

The reason this is not "move the fsync outside the lock" is that today the lock
gives the cache several guarantees for free. All must survive:

1. **Durability before visibility.** A value becomes visible in the index (to
   `get`) and to the watch observer only after it is durable. Committing outside
   the lock naively would expose a value before its fsync, and a reader could see a
   put that a crash then loses. The stream path avoids this by deferring the
   in-memory apply until after `commit` + `turn.wait()`; the cache must do the same.
2. **Watch order = shard write order.** `cache_watch` relies on the observer
   firing in the shard's write order (the comment in `put_checked`: "Observed
   while the state lock is still held … that hold is what makes the order watchers
   see the shard's order"). Watchers deliver offsets; a watcher that saw B before
   A on a shard whose log reads A,B would look like a lost write. Ordering must be
   re-established by disk offset (the sequencer), not by lock hold.
3. **Index consistency.** For the same key, a later offset must win. Index updates
   applied out of order would leave a stale value pointing at the wrong offset.
4. **Recovery is unchanged.** The index is derived from the log and rebuilt on
   open; the fix must not change on-disk format or the recovery contract
   (`docs/durable-storage.md`, `docs/storage-format.md`).
5. **Compaction correctness.** Compaction currently runs under the same lock
   (`should_compact` → `compact`). It rewrites the shard directory and swaps the
   log; it must be serialised against in-flight appends (it cannot run while an
   `append_pending` for the old log is uncommitted).
6. **`StorageApi::put` ack semantics.** `put` returns `()` and logs on failure
   ("the caller has been told the write succeeded"). The fix must keep the point
   at which the caller is considered acked = **after the value is durable**
   (i.e., `put_checked` still returns only post-commit).

## Design

Give each cache shard the stream path's shape: a **per-shard commit sequencer**,
`append_pending` + `commit`, and an ordered in-memory apply.

Restructure `put_checked` (and `delete_checked`) to:

1. **Short lock** on `ShardState` (call it the *append lock*):
   - `ensure_index`.
   - `append_pending(op.encode())` → `PendingAppend` (claim offset, no fsync).
   - `let turn = shard.commit_sequencer.reserve(first, last+1)`.
   - Record what the index/observer update will be (key, offset, bytes, value,
     expiry) as a small owned struct, but **do not apply it yet**.
   - Release the append lock.
2. **Outside the lock**: `commit(&pending)`: group-committed fsync, overlaps with
   every other concurrent writer's commit.
3. `turn.wait()`: block until all lower offsets on this shard have applied.
4. **Apply lock** (may be the same mutex, re-acquired): apply the index mutation
   for this offset, then `notify` the observer. Because `turn.wait()` guarantees
   this runs in offset order, index and watch order match disk order. Release.
5. Return `Ok(())` (post-commit ⇒ the caller's ack is still durability-gated).

New state per `CacheShard`:

- `commit_sequencer: CommitSequencer`: reuse `felix-broker`'s if it can be moved
  to a shared crate, or lift the sequencer into `felix-storage` (preferred: it is
  storage-ordering, not broker logic). Decide during implementation; a small,
  self-contained copy in `felix-storage` is acceptable if a shared crate is
  churn.

Compaction: gate it on the sequencer being idle (no reserved-but-unapplied
turns), and take the append lock for the swap. Simplest correct version: run
compaction only from the ordered-apply step (step 4) when `should_compact`, where
we already hold the apply lock and know all lower offsets are applied. This keeps
compaction exactly as serialised as today relative to *applied* state, while
appends for higher offsets that have not yet applied are naturally behind it in
the sequencer.

Failure/cancel handling: mirror the stream path. The `turn` must release its
range on every exit path (`?`, panic, drop mid-await), or a failed commit strands
the shard (later writers wait on a turn that never arrives). The broker solved
this by holding the `turn` in a guard whose drop releases it; reuse that shape.

## Implementation steps

1. Lift or copy a minimal `CommitSequencer` into `felix-storage` (per-shard
   instance on `CacheShard`). Verify the drop-releases-range guard is preserved.
2. Add `append_pending` usage to `CacheShard::write` split: a `stage()` that does
   `append_pending` + returns the pending + the pending index mutation, and an
   `apply()` that mutates the index. Keep `write()` for the compaction path (which
   is single-writer and does not need the split).
3. Rewrite `put_checked` / `delete_checked` to the 5-step flow above.
4. Move the observer `notify` into the ordered `apply()` step.
5. Re-home compaction into the ordered-apply step (or gate it on sequencer idle).
6. Audit `ensure_index`, `covered_through`, and `should_compact` for assumptions
   that the whole write ran under one lock hold.

## Testing

- **Throughput (the bug):** a concurrency harness (N writers to one shard under
  `FsyncMode::OnCommit`) asserts aggregate puts/s scales with N (not flat).
  Revert the split to confirm it fails (flat ~1/fsync), the way a regression test
  should. The felix-loadgen `cache` scenario already exercises this end to end.
- **Ordering:** concurrent writers to the *same key*; assert the final value and
  the observer's last-seen offset are the highest offset. Concurrent writers to
  different keys; assert the observer sees strictly increasing offsets.
- **Durability before visibility:** with a fault-injecting fsync (fail the flush),
  assert the value is **not** visible to `get` and **not** delivered to a watcher.
  Visibility is still gated on a successful commit.
- **Crash recovery:** unchanged on-disk format; existing recovery tests must pass,
  plus one that kills mid-window (some committed, some not) and checks the rebuilt
  index matches the committed prefix.
- **Compaction under load:** writes concurrent with a compaction trigger; assert
  no lost or duplicated keys and offsets stay monotone.
- **Cancellation:** drop a `put_checked` future mid-commit; assert the next
  writer's `turn` still fires (no strand).

## Risks and alternatives

- **Risk: subtle ordering/visibility regression** in durability-critical code.
  Mitigated by reusing the *proven* stream-path sequencer rather than inventing a
  new scheme, and by the fault-injection + ordering tests above.
- **Alternative considered: coarse batching timer** (accumulate puts for a few ms,
  one fsync): simpler, but adds latency to the uncontended case and still needs
  ordering care; the sequencer approach adds no latency when there is no
  contention and matches the path the codebase already trusts. Rejected.
- **Alternative: per-key locks instead of one shard lock:** does not help; the
  bottleneck is the fsync, not key contention, and different keys already share
  the one log and its one fsync.

## Expected outcome

Concurrent cache-put throughput rises from the flat ~230/s (1/fsync) toward the
group-commit ceiling the stream path already reaches (one fsync serving many
writers), while durability, watch order, and index consistency are unchanged.
Single-writer latency is unaffected (no batching delay when uncontended).
