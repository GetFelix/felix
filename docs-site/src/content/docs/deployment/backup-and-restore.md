---
title: "Backup and restore"
---

A tar of a running broker's volume is not a backup of the cluster. Each
broker's copy is taken at a different moment, a shard's leader may hold
records a majority never acknowledged, and nothing ties the copies to the
control plane's metadata. A **backup point** is what ties them together: one
committed offset per shard log, read from each shard's leader after a single
barrier instant. Copies of the leaders' shard directories, taken while the
brokers keep running, are cut back to the point on restore.

## What a backup point guarantees

- **Nothing acknowledged before the barrier is missing.** The barrier instant
  is recorded first, and every offset is read after it. A record acknowledged
  before the barrier is below the offset read for its shard.
- **Nothing uncommitted is included.** Each offset is the shard's committed
  offset on its leader: for a `Quorum` stream or cache, the quorum mark (and
  never past what the leader itself has made durable); for everything else,
  what the leader has acknowledged, which under
  `FELIX_DURABLE_FSYNC_MODE=on_commit` stops at the last synced record.
  A Raft shard's offset is one past the last record its leader
  acknowledged. That can be past its commit for a moment: a `Quorum` stream
  under a periodic fsync answers once a majority has written a record, and
  a `Leader` stream answers on the leader's lease. Such a record is in the
  point, since leaving it out would lose an acknowledged write.
- **Every shard is at its leader's copy, at one generation.** The assignments
  are read again after the offsets. If any leader or generation changed while
  offsets were being read, the whole collection starts over; after five tries
  the command fails rather than write a point from leaders that have since been
  replaced. A shard its leader cannot vouch for yet (just taken, lease lapsed)
  is asked about again a few times, and then the command fails naming it. A
  partial point is not a point.
- **A Raft shard is read on its elected leader.** Every broker is asked each
  round, and a shard is read as a Raft one when any broker lists a member
  of it. That is decided again every round, so a shard whose members are
  not listed yet is asked about again rather than read on the broker
  placement names, which need not lead it.
- **A Raft shard needs a majority of its brokers reachable.** Its point is
  taken only when most of the brokers that vote in its group answered in
  the same round, and while a membership change is in progress, most of the
  old set and most of the new one. A broker that has lost its leadership
  without hearing so yet cannot then pass for the leader, because one of
  the brokers that answered has already seen the newer leader. A broker
  that only copies the log and does not vote counts for nothing. Short of
  a majority the shard is asked about again a few times, and then the
  command fails naming it. A single-broker shard needs only that broker.

"Acknowledged" means what the ack promised. A durable `Leader` publish is acknowledged
after the write unless the broker runs with `FELIX_ACK_ON_COMMIT=0`, so with
that set, a record acknowledged just before the barrier may not yet be in
any log. See
[delivery semantics](https://github.com/GetFelix/felix/blob/main/docs/semantics.md).

## What it does not guarantee

- **The point is not one instant across shards.** Offsets are read one
  leader at a time, after the barrier, with no write pause. Every shard is at a
  committed prefix and nothing acknowledged before the barrier is missing, but
  a record written *during* the collection can be in the point on one shard and
  not on another. Felix orders nothing across shards, so no Felix guarantee is
  broken, but a client that published to shard B after an acknowledgement from
  shard A during that window can see B's record without A's after a restore.
  The window is one HTTP request per broker; keep placement quiet while taking
  a point and it stays short.
- **A `Leader` shard is cut at what its leader had.** A `Leader` stream's
  committed offset is its leader's acknowledged tail, which a failover could
  have lost; the point captures what the leader held, not what would have
  survived.
- **Caches, consumer groups and counters compact.** Their logs are rewritten
  into a fresh log appended at the tail. A copy taken after a compaction that
  followed the point no longer holds the log as it stood at the point, and the
  restore refuses it (`offset … is outside this log`). Copy those directories
  first, right after taking the point; if a restore refuses one, take a new
  point and copy again.
- **Brokers with no cluster** have no assignments, so there is nothing to take
  a point of. Stop a single broker and copy its data directory instead, and
  restore it as described under
  [Restoring a standalone broker](#restoring-a-standalone-broker).

## Taking a backup

1. **Take the point.** It needs `node.view:cluster:*`, the permission that
   lists shard assignments.

   ```bash
   export FELIX_CONTROLPLANE_URL=http://felix-controlplane:8443
   export FELIX_CONTROLPLANE_TOKEN=<a Felix token>
   felixctl backup-point nightly-2026-09-27
   ```

   ```text
   backup point "nightly-2026-09-27" at 1790467200000 ms, metadata version 4182
   SHARD                   LEADER    GENERATION  RECORDS  CURSORS  DEAD_LETTERS  COUNTERS
   t1/ns/orders/0          broker-1  7           918233   12       0             -
   t1/ns/orders/1          broker-2  5           901112   12       0             -
   t1/ns/prices/0 (cache)  broker-0  3           44120    -        -             310
   wrote nightly-2026-09-27.backup-point.json
   ```

   Each broker's offsets come from `GET /backup/offsets` on its metrics
   listener (see [Broker API](/api/broker-api/#backup-offsets)), at
   `http://<the host of its client address>:8080`. `--metrics-port` changes the
   port for every broker, and `--broker broker-1=http://10.0.4.7:9090` names
   one broker's URL outright. `--out` names the manifest file.

2. **Copy each leader's shard directories while it runs.** For every shard in
   the manifest, from the broker the manifest names as `leader`, under that
   broker's `FELIX_DURABLE_STORAGE_DIR`:

   | Log | Directory |
   |---|---|
   | stream records | `<root>/<shard dir>` |
   | cache records | `<root>/caches/<shard dir>` |
   | consumer-group cursors | `<root>/groups/<shard dir>` |
   | dead letters | `<root>/dead-letters/<shard dir>` |
   | counters | `<root>/counters/<shard dir>` |

   The shard directory is named after the tenant, namespace, name and shard
   (`t1_ns_orders_0-<hash>`; see
   [the storage format](https://github.com/GetFelix/felix/blob/main/docs/storage-format.md#directory-layout)).
   Within each directory copy the small files first (`durable.mark`,
   `replica`, `epochs`, `producers`), then the `.log` segments oldest first,
   then the `.index` files. That order keeps every file from claiming more
   than the segments copied after it hold. The copy may run past the point:
   records are never rewritten in place, so the bytes below the point are
   final, and whatever lands after them is cut on restore. A torn last record
   is repaired when the copy is opened. Copy the cache, group and counter
   directories first, for the compaction reason above.

3. **Store the manifest with the copies.** A copy without its manifest cannot
   be cut back to a committed point.

4. **Back up the control plane's metadata** from a state taken at or after the
   point, so every stream and assignment the point names exists in it. That is
   a storage-layer snapshot of the members' volumes (see
   [Metadata Raft](/architecture/metadata-raft/) and the Kubernetes
   [Backups](/deployment/kubernetes/#backups) notes). The manifest's
   `metadata_version` is the assignment change sequence the point was read
   against.

A broker still running when the control plane's metadata is restored needs
no restart, even if it registered after the backup was taken. Its next
heartbeat answer asks it to register again, and it does without giving up
its lease. A broker that is draining is not asked while the drain lasts.

## Restoring

On the default engine, stop the broker, put its copied shard directories under
`FELIX_DURABLE_STORAGE_DIR`, run `felixctl data restore-point --point FILE
--node <broker>` on it, and start it. Each log is cut back to the point and the
directory gets a new storage id. A Raft cluster takes the steps under
[Restoring a Raft cluster](#restoring-a-raft-cluster).

:::note[Offsets past the point on Raft]
The re-form a Raft restore runs, `felixctl raft reform plan --restore-point
FILE`, starts each shard's next epoch at the larger of the point's end and 2^14
offsets for every millisecond of the control plane's clock at plan time plus
an offset gap (2^40 by default and at most, `--offset-gap N` to choose
another, below 2^32 only with `--accept-short-gap`). The point's own clock is
not used. Records acknowledged after the point are lost with the restore, but
their offsets are not given out again, also when you restore from the same or
an older point a second time, as long as a shard has acknowledged at most
2^13 offsets per millisecond on average since the last restore and the
control-plane clocks agree within about four minutes. A shard restored again
within about four and a half minutes of its last restore is blocked until
the line has moved far enough past it; with a smaller gap than that restore
used, the wait is longer. A plan whose clock puts that line below the point's
end is refused unless you pass `--accept-short-gap`. A reader that kept an
offset past the point can tell from it that the record is gone, and new
records never take it. The gap is a header in the next segment, not disk
space.
:::

### Restoring a Raft cluster

A Raft shard has no leader to copy back into. The restore cuts the leader's
copy at the point, wipes every other copy, and re-forms each group from the
cut copy, with `felixctl raft reform` and the point. Anything acknowledged
after the point is gone, and its offsets are not given out again (see the
offset gap above). Run these in order.

1. **Stop every broker, and restore the control plane's metadata** from the
   backup taken at or after the point.

2. **Put back only the leaders' copies.** On each broker, start from an empty
   `FELIX_DURABLE_STORAGE_DIR`, and copy in the shard directories the
   manifest names that broker as `leader` of, from the backup in step 2 of
   [Taking a backup](#taking-a-backup). A broker that led nothing in the
   point stays empty. These empty copies come back as learners of the
   re-formed group. A cache shard's learner catches up from the leader's
   snapshot and becomes a voter again. A stream shard's learner cannot catch
   up yet, so a restored stream shard runs on its one cut copy until
   [#3005](https://github.com/GetFelix/felix/issues/3005) lands: it serves and
   takes writes, but with no second copy.

3. **Cut each leader's copy to the point**, on the broker that holds it, with
   the broker stopped. `--node` takes the broker's own id from the manifest.

   ```bash
   felixctl data restore-point --point nightly-2026-09-27.backup-point.json --node broker-1
   ```

   The output looks like this (illustrative, not captured from a run; the
   numbers are made up):

   ```text
   new storage id 7d1c0e5a...
   t1/ns/orders/0 Stream (raft): -> 918233
   restored 1 logs to point "nightly-2026-09-27"; start this broker, ...
   ```

   The line marked `(raft)` is a Raft copy cut at the point. The command
   refuses, and writes nothing to that copy, when the copy holds a snapshot,
   a purge or a floor at or past the point: a copy taken after the point
   moved on cannot be cut back. It also refuses when the record below the
   point was written in another term than the point records: the leader
   lost a `Leader` write and a newer leader wrote over it before the copy.
   Take a new point and copy again.

4. **Start every broker.** The Raft shards stay "not ready" and wait for a
   re-form; nothing is lost by waiting.

5. **Plan the re-form from the point.** This reads each broker and changes
   nothing.

   ```bash
   felixctl raft reform plan --restore-point nightly-2026-09-27.backup-point.json
   ```

   Illustrative output, not captured from a run:

   ```text
   plan 3f2a9c (all): done; 1 to re-form, 0 blocked, 0 left alone
   restore from backup point nightly-2026-09-27: no offset below the point plus 1099511627776 is given out
   SHARD           EPOCH  STATE   SOURCE    VOTERS    NOTE
   t1/ns/orders/0  0      reform  broker-1  broker-1  MAY LOSE ACKNOWLEDGED RECORDS: ...
   1 of these could lose acknowledged records; applying them will need --accept-loss
   ```

   The cut copy is the only voter: the wiped copies hold nothing the plan can
   vouch for, so they start as learners.

   A shard is `blocked`, with the reason, if the point does not name it, names
   another incarnation of it, or no restored copy ends at the point. Fix the
   cause (usually a missing or uncut leader copy) and plan again.
   The new epoch starts at 2^14 offsets per millisecond of the control
   plane's clock at plan time, plus a gap (`--offset-gap N`, 2^40 by
   default). That keeps a later restore, from this backup or an older one,
   past every offset an earlier restore gave out, as long as a shard stays
   under 8 offsets per microsecond and the control-plane clocks agree within
   about four minutes. A shard whose last epoch began less than about four
   and a half minutes before the plan is `blocked` until the line clears it;
   plan again then. The point's own time comes from the
   machine that took it and is only shown. A plan made before another plan
   that already restored the shard is refused as `stale`; use the plan that
   won, or plan again once the control plane's clock is past its time.

6. **Apply it.** The plan says it may lose records, because the wiped copies'
   votes are unknown, and what it loses is what was acknowledged after the
   point. Name each shard to accept that.

   ```bash
   felixctl raft reform apply 3f2a9c --accept-loss t1/ns/orders/0
   felixctl raft reform apply 3f2a9c --accept-loss cache:t1/ns/sessions/0
   ```

   It prints each shard as `started` once its group has formed.

7. **Verify.**

   ```bash
   felixctl raft reform plan
   ```

   Every shard reads `left_alone`: each has a leader again. Then read a shard
   back:

   ```bash
   felixctl sub orders --shard 0 --from earliest --format offsets --count 5
   felixctl cache get sessions some-key
   ```

   A stream returns the records the point held, and the first record
   published after the restore arrives at an offset at least the point plus
   the gap. A cache returns its restored values.

:::caution[Never touch one shard directory]
Do not delete, empty, move or restore a single shard directory under a broker's
data directory. To recover a broker's storage, stop the broker and replace or
wipe its whole data directory. On the deprecated hybrid engine the broker keeps its storage id, its copy
trust and its kept ballots in the data directory root, so if only a shard
directory changes, it still reports that shard's copy as kept and trusted,
and a promotion can count an empty copy and lose an acknowledged write
([#2376](https://github.com/GetFelix/felix/issues/2376)). Raft detects a
shard directory put back to an older state, except one put back to a state
from the last second with no election since. That window is longer while the
mark writer is backed up, up to about six seconds. A whole data directory put
back in the same run to such a state is not detected either, so restore whole
data directories only while the broker is stopped.
:::

## Restoring a standalone broker

A broker with no `FELIX_NODE_ID` keeps everything in its one data directory,
so its backup is a copy of that directory, taken with the broker stopped. A
copy taken while it runs is not a consistent point: its files are copied at
different moments, and copying takes no lock, so nothing refuses it.

1. Stop the broker.
2. Replace its whole `FELIX_DURABLE_STORAGE_DIR` with the copy.
3. Run the restore tool on it, with the broker still stopped. This step is
   required: a copy put back without it is not detected, and the broker can
   hand out again offsets it already handed out. `felix-broker restore-point`
   counts too, but a standalone broker has no backup point to give it.

   ```bash
   felix-broker rotate-storage-id --dir /var/lib/felix
   ```

   Both tools lock the data directory, as a running broker does, so either
   one refuses to run while a broker still holds it, with an error naming
   the directory.

4. Start the broker.

Each stream and cache shard starts again from the records in the copy.
Anything acknowledged after the copy was taken is gone, and so are its
offsets: the first record published after the restore gets an offset of at
least 2^14 per millisecond of the broker's clock plus 2^40. A consumer that
kept an offset from after the copy finds no record there, and no new record
ever takes it. This holds under every fsync mode, as long as a shard
averaged at most 2^13 offsets per millisecond since its last restore and the
broker's clock has not gone back between the two restores. If the clock was
set back, fix it, or wait until it is past the time of the last restore,
before starting the broker.

:::caution[Always run the tool]
Step 3 is what tells the broker the directory was put back. Nothing inside
the directory can show that on its own, so a copy started without it, a
rolled-back volume snapshot or a restored VM included, hands out again the
offsets it handed out after the copy was taken. A reader that stored one of
them then reads a different record there.

If the tool stops part way, it leaves `restore-pending` in the directory and
the broker refuses to start until you run the tool again. Deleting
`storage-id` by hand is not the same: it records no restore, and the
broker's shards stay closed.
:::

## Rebuilding one broker's copy

This works today, as long as every shard the broker holds has other healthy
copies.

1. Stop the broker.
2. Replace its whole data directory (`FELIX_DURABLE_STORAGE_DIR`) with an empty
   one, or with a copy from a backup. If you put a copy back, delete
   `<FELIX_DURABLE_STORAGE_DIR>/storage-id` first, or run
   `felix-broker rotate-storage-id --dir /var/lib/felix` with the broker
   stopped, so the directory gets a new storage id.
3. Start the broker.

Each shard's copy on that broker comes back with a new copy id. A member with
a new id has never voted, so it joins its group as a learner and catches up
from the leader, and the group's membership change then removes the old
member. See "Group and member identity" in
[per-shard Raft](https://github.com/GetFelix/felix/blob/main/docs/per-shard-raft.md#group-and-member-identity).
This costs one full copy of each shard the broker holds.

It does not recover a shard that lost its majority, and it does not work if
every copy of a shard is rebuilt this way: for that, restore from a backup point as described above.

## What to keep

Keep taking backup points and copying the leaders' shard directories as
described above, and keep the metadata backups. `felixctl backup-point` and the
live copies change nothing on the cluster, and the Raft restore above is the
way back from them.

## See also

- [Durable storage](https://github.com/GetFelix/felix/blob/main/docs/durable-storage.md#restoring-to-a-backup-point)
  for why a live copy is safe to take and what `restore_to` does to a log.
- [Moving shards by hand](/deployment/moving-shards/) for
  `felixctl placement pause`, which keeps placement quiet while a point
  is taken.
