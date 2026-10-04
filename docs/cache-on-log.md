# The cache is a projection over the log

**Decision: a cache is a log. `put` and `delete` append records, an in-memory
index maps key to offset, and `get` reads the log. The separate in-memory
key/value store is kept only for brokers configured without durable storage.**

This is what `docs/architecture.md` has claimed from the start: "one core log,
many semantics", with the cache as "key → latest value with TTL, backed by the
same log". Until now that was aspiration stated as description: the cache was a
`RwLock<HashMap>` alongside the stream registry, sharing no code, no durability,
and no replication with it.

## What the log already gives, free

Everything a durable cache needs is already built and already tested, because
streams needed it first:

- **Crash safety.** CRC-verified records, torn-tail repair, interior corruption
  refused rather than silently accepted.
- **Group commit and fsync policy.** A cache write is an append, so it inherits
  the same throughput lever. The write path is shaped like the stream publish
  path to actually reach it: a short lock claims the offset and a place in the
  apply order (`felix-storage`'s `CommitSequencer`, shared with the broker),
  the fsync runs outside any lock so concurrent writers share one flush, and
  the index update and watch notification apply strictly in offset order
  afterwards. Durability still gates the acknowledgement *and* visibility: a
  staged write is invisible to `get` and to watchers until its commit
  completes and its turn arrives.
- **Replication.** The driver ships log records and does not care what semantic
  reads them, so the shipping, quorum accounting, and catch-up tracking are all
  reusable as they stand. What is *not* free is opening the right log: the
  driver resolves a shard through `open_stream`, and a cache's log lives under
  the cache root instead. Replicating a cache needs that one seam widened, not a
  second replication path.

That last point is the argument for doing it this way rather than bolting
durability onto a hash map: the alternative is a second durability path and a
second replication path, which is exactly what the storage design set out not to
have.

## Where a cache lives on disk

Under `<root>/caches/`, with a provider of its own; streams stay at `<root>/`.

A shard directory is named from a hash of `(tenant, namespace, stream)`, so a
cache and a stream with the same name in the same namespace would otherwise
land in the same directory and interleave their records. Separating the roots
makes that impossible rather than unlikely, costs nothing, and leaves the
existing stream layout untouched, with no migration because no stream path
changes.

Nothing enumerates the storage root (recovery scans a *shard* directory for
segment files), so an extra subdirectory under it is inert.

## The record format

One record per write. Little-endian, and versioned in its first byte, because
this is a durable format and a reader years from now has only these bytes.

<p align="center">
  <img src="assets/storage/cache-record.svg" alt="Cache record byte layout: a one-byte version, a one-byte op, an eight-byte absolute expiry, a four-byte key length, then the key and, for a put, the value" width="760">
</p>

- `op` is `0` for a put and `1` for a delete. A delete carries no value.
- `expires_at_millis` is absolute Unix milliseconds, `0` meaning "never".
  Absolute rather than a duration: a record read back after a restart has to
  mean the same thing it meant when written, and a duration would silently
  restart its life on every recovery.
- `key_len` bounds the key so the value needs no length of its own. It is
  simply the rest of the record, which the segment framing already delimits.

An unknown version is refused rather than guessed at, for the same reason an
unknown frame flag is: a misread record is worse than an unreadable one.

## The index is derived, never trusted

`key -> (offset, expires_at)`, held in memory and rebuilt by replaying the log
when the cache is opened. This is the same rule the segment indexes follow, and
for the same reason: anything that can be recomputed from the log must be,
because then it can never be stale in a way that matters.

**The index holds offsets, not values.** The log is the store; memory holds only
where to look. This costs a read per `get` that a hash map would not pay, and it
is what makes the durability real rather than a write-behind of an in-memory
map that is still the actual source of truth. Caching hot values in memory in
front of this index is a straightforward later optimisation, and deliberately
not part of making the claim true.

## TTL

Checked at read against the record's own `expires_at_millis`, so an expired
entry is reported as absent the moment it lapses. Separately, the shard's
leader writes a delete for each entry whose TTL has passed
(`LogCache::expire_due`), through the same fence a client's write takes and
replicated like one. That delete is what tells a watch the entry is gone, and
lets compaction drop it. It is staged under the lock a put stages under and
checks the expiry again there, so a put that refreshed the key after it was
found due keeps its value. An in-memory cache still expires lazily only.

The leader's expiry pass runs once a second and writes at most 1024 deletes per
shard per pass, so under a mass expiry the deletes lag the TTLs by as many
passes as it takes. The fence and lease are checked before each delete, and a
pass stops once either says the shard is no longer this broker's to write.
Expiry is decided by the leader's clock and is permanent once written: a leader
whose clock jumps forward deletes entries early, and a later fix to the clock
does not bring them back.

## Compaction

Without it the log grows forever, and "the cache is a log" would be a slow leak
rather than a design.

Compaction copies the live set (the newest surviving record for each key that
is neither deleted nor expired) forward to the tail, then deletes the sealed
segments that held only superseded records. **Records are never rewritten**,
which is the invariant everything in the storage layer rests on: a copy is a new
put of the same value, and whole segments are dropped from the head, the way
retention drops them.

It runs when the log has grown past a multiple of its live bytes, so the cost is
proportional to the garbage and a cache that is mostly live is never compacted.
It runs on a background task with its own I/O budget, never on a write: a pass
seals the active segment to fix a cut, copies what is live below it in small
batches staged and committed like writes, flushes, and deletes the segments
below the cut. A key with a write in flight is not copied, because the copy
would land after the write and undo it on replay. `docs/durable-storage.md`,
"Cache and counter compaction", has the steps and the crash analysis.

**Compaction continues the offset space rather than restarting it.** The live set
is appended at the tail, so an offset names the same record for the life
of the shard even across many compactions and restarts. Two things depend on
this. A reader tracking offsets never sees them go backwards. And replication
ships records *at* their offsets, so a leader that renumbered on compaction would
make its offset 0 a different record from every follower's: two logs that have
silently diverged, with nothing in either able to detect it.

The cost is that offsets are sparse after a compaction: the numbers the reclaimed
records held are never reused. That is the same shape a stream's log takes after
retention trims its head, and nothing reads a cache by offset anyway.

## Routing

A cache key hashes to a shard with the same function a routing key uses for a
publish, and that shard has exactly one owner. A broker that receives an
operation for a key it does not own forwards it to the owner over the internal
protocol and relays the answer, the way a publish already does.

This is what makes a cache usable from more than one broker. Before it, every
broker served its own copy: a `put` on one was invisible to a `get` on another
and, worse, two brokers could hold *different* values for one
key with nothing to reconcile them. A miss is detectable; divergence is not.

Three details are load-bearing:

- **A cache and a stream may share a name**, and when they do they are unrelated.
  Everything keyed by a shard carries the kind for that reason: the control
  plane's assignments, the broker's ownership table, the router's routes, and
  the width each is resolved against. Any one of them dropping it would file a
  cache's shard under the stream's and answer for it.
- **An unroutable operation is refused, not served locally.** A local write for
  a key this broker does not own is the divergence, so it is a refusal even
  though refusing is the less helpful answer.
- **A failed read is an error, not a miss.** Reporting a miss would let a client
  conclude a key does not exist when it does, on the owner.
- **A failed write is an error, never an acknowledgement.** A put or delete the
  owner's log refused (a failed fsync poisons it) is in no read's future, so
  acknowledging it would let a later read return an older value.

## Delete

`CacheDelete` is answered with the value it removed, so a caller learns whether
the key was there without a second round trip. Removing a key that was never
present is an answer rather than a failure.

It is negotiated: the broker advertises `FEATURE_CACHE_DELETE` in `AuthOk`, and
a client that does not see the bit reports that the broker cannot delete instead
of sending the request. That direction matters: an unrecognised message type
ends a broker's control loop, so probing an older one costs the connection
rather than returning an error.

The feature is advertised by a standalone broker as well as a clustered one.
Only the cluster-shaped features, topology and redirect, depend on there being
a cluster to describe.

## Watch

A key or key prefix can be subscribed to, like etcd's watch, over the cache's own
log. `cache_watch` is negotiated (`FEATURE_CACHE_WATCH`) and delivers each
applied write in the shard's write order: a put with its value, a delete as a
change with no value, each carrying the log offset the write was appended at.
It is pure composition of what already existed: the log gives every change an
offset, the index gives current state, and the per-subscriber
bounded-queue-with-`try_send` discipline gives fanout that never blocks a
writer. `docs/protocol.md` specifies the messages; what follows is the
reasoning.

**The write order watchers see is disk-offset order.** The store reports each
applied write to an observer under the shard's apply lock, and applies run
strictly by offset, through the same commit sequencer the stream publish path uses.
Fsyncs overlap across concurrent writers (that is what group-commits them), but
what the index and every watcher observe is re-serialised into the order the
log reads. The observer must not block there, so fanout is `try_send` against
bounded queues, following the same publisher-never-blocks rule stream fanout follows.

**The join is register-before-read.** A watch resuming from an offset registers
its queue first, then reads the shard's applied frontier (the log's tail, or
lower while writes are in flight), then serves `[from_offset, frontier)` from
the log. Registration pins the live edge: every change below the frontier is
applied and on disk, and every change at or past it is still to be applied and
will be queued, so the two halves cannot leak a write landing in between. The
frontier and not the tail, because a write already in the log but not yet
applied is in no snapshot, and with the tail as the edge its change would
arrive below it and be dropped. It is the same discipline, for the same past
defect, as the stream resume path. The registration race is closed by offsets:
a change applied just before the frontier was read can arrive both in the
replay and on the queue, and the queued copy is dropped because its offset is
below the frontier.

**Falling behind ends the watch, loudly.** A stream subscriber reads a queue
drop out of a jump in delivered offsets. A *filtered* watch cannot: other
keys' writes make its offsets sparse, so any gap looks like traffic it was not
watching. A watcher whose queue overflows is therefore ended rather than
thinned: everything already queued is delivered, then `cache_watch_lagged`
names the offset of the first missed change, and re-watching from it is
gapless. Silent loss is the one outcome the contract rules out.

**Compaction interplay is the resnapshot rule.** A watch resuming from an
offset older than the log's base (compaction collapsed that history) cannot
be replayed, and pretending otherwise would silently skip it. Instead the
broker answers `resnapshot: true` and delivers each matching key's *current*
value at its offset, then live changes: the snapshot-plus-changes shape the
control plane's assignment watch already uses. Compaction itself is silent to
watchers. It moves where live records sit without changing what the cache
holds, so notifying would report phantom writes.

**Retained delivery is the snapshot pointed at establishment.** A watch that
asks for `retained` receives each matching key's current value first (MQTT's
retained message), then live changes: a client joins and immediately holds the
state instead of waiting for the next write, which is the primitive presence
and state-sync applications are built on. It is literally cache-read composed
with the watch's join: the same index snapshot the compacted-resume path
serves, delivered at establishment instead of as a fallback, under the same
register-before-read discipline. The confirmation carries `retained_count`, so
joining an empty key is a definite `0` rather than a silence
indistinguishable from a slow key, and the client knows the exact moment
its state is complete. A key whose newest write races past the live edge
mid-join can be absent from the retained set; its change is already queued and
arrives as the first live event, folding to the same state, with the offset
making the situation legible. Refused together with `from_offset`: the replay
already reconstructs the state a retained start shortcuts, and serving both
would deliver every value twice.

**TTL expiry is a change.** When an entry's TTL passes, the shard's leader
writes a delete for it in its next expiry pass (see [TTL](#ttl): at most 1024
per shard per pass, so a mass expiry lags), and a watch delivers it like any
other delete (no value). The put's `expires_at_millis` still travels with the
put's event. A watch replaying history also replays writes whose TTL has since
lapsed, followed by their deletes, because they are the history.

**Watches are served where writes are applied.** A watch belongs to the shard's
owner, and a broker that does not own the shard redirects (`not_leader`) rather
than proxies, exactly as a subscribe does. Records replicated to a follower
bypass `put` and fire no observer, which is correct: the follower serves no
watches, and a client whose broker fails re-establishes against the promoted
owner by offset. The rebuilt index and continued offset space are what make
that resume land exactly where the old watch left off.

## Counters, beside the cache

A counter is addressed like a cache key and routed like one, but lives in a
store of its own under `<root>/counters/`. A new durable record shape gets a
new root, so a build that predates it never meets bytes it cannot read in a
log it already serves. `docs/projections.md` owns the design; the load-bearing
consequence here is that a counter and a cache value may share a key and are
unrelated, and a cache watch does not see counter changes.

## Replication

A cache's shards are replicated by the machinery that replicates a stream's: the
leader ships records at their offsets and a follower checks each batch begins at
its tail. Everything that makes that safe (the generation check, the gap and
divergence answers, bootstrapping a follower whose history has been trimmed) is
the same code, because a cache shard *is* a log.

Three things had to be true for that to work.

**Offsets never rewind.** Compaction appends the live set at the tail rather than
renumbering from zero, or a leader's offset 0 would be a different record from
every follower's.

**The kind travels with the batch.** `ReplicateCacheRecords` is a distinct
message kind with the same body as `ReplicateRecords`, so a follower cannot
append a cache's records into the stream of the same name. A separate kind
rather than a field on the shard reference, because the internal protocol
evolves by adding kinds. Widening an existing body needs a version bump, and a
version bump makes a rolling upgrade impossible.

**The index catches up.** A follower is shipped records without going through
`put`, so its in-memory index knows nothing about them. The index now reads
forward to the log's tail whenever it is behind, rather than building once and
trusting itself; without that, a promoted follower answers misses for values it
is holding on disk.

**A promotion fences both logs.** A promoted cache leader fences a majority of
the replica set on the cache log and then on the counter log, takes the
furthest ahead of each, and writes a generation-start record on each before it
serves, as a stream leader does with its one log. Dropping records from a log,
whether a follower drops a divergent suffix or a promoted leader takes a longer
log in place of its own, also drops the cache's index or the counter sums,
which are rebuilt from the log as it now is; an index still pointing at a
dropped offset would read whatever record replaced it. With `fenced_caches`
finalized a `Quorum` cache acknowledges on its followers' answers without the
lease. See [replication-design.md](replication-design.md#fencing-a-promotion).

## What this does not do yet

**Warming on takeover.** A stream's log is opened while the shard is being
taken, so a torn tail is repaired before the shard is declared servable. A cache
shard's log is opened lazily on the first request that touches it, so the same
repair happens later and inline.

**`Leader` and `Quorum` for a cache read.** A cache declares a consistency
level for its writes, as a stream does: under `Quorum` a put or delete is
acknowledged only once a majority of the shard's replicas hold it. What either
level should mean for a *read*, and what a client should expect of
read-your-writes after a failover, is undecided.
