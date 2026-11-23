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
  before the barrier was committed before the barrier, so it is below the
  committed offset read for its shard.
- **Nothing uncommitted is included.** Each offset is the shard's committed
  offset on its leader: for a `Quorum` stream or cache, the quorum mark (and
  never past what the leader itself has made durable); for everything else,
  what the leader has acknowledged, which under
  `FELIX_DURABLE_FSYNC_MODE=on_commit` stops at the last synced record.
- **Every shard is at its leader's copy, at one generation.** The assignments
  are read again after the offsets. If any leader or generation changed while
  offsets were being read, the whole collection starts over; after five tries
  the command fails rather than write a point from leaders that have since been
  replaced. A shard its leader cannot vouch for yet (just taken, lease lapsed)
  is asked about again a few times, and then the command fails naming it. A
  partial point is not a point.

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
  a point of. Stop a single broker and copy its data directory instead.

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

## Restoring

:::caution[Restoring a whole cluster from a backup point is not supported on Raft clusters yet]
`felixctl data restore-point` gives each broker a new storage id, and every
Raft shard directory then opens as a new member that has never voted. A group
is never formed again on such copies, so every shard answers "not ready"
after the restore. The only way out today is to delete and recreate the
streams, which loses their records. Do not restore a Raft cluster this way
until [#2983](https://github.com/GetFelix/felix/issues/2983) ships. Per-shard
Raft's [re-form rules](https://github.com/GetFelix/felix/blob/main/docs/per-shard-raft.md#re-forming-a-group)
explain why.
:::

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
every copy of a shard is rebuilt this way: that is the fleet-wide case above.

## What to keep

Keep taking backup points and copying the leaders' shard directories as
described above, and keep the metadata backups. `felixctl backup-point` and the
live copies change nothing on the cluster, and the whole-cluster restore in
[#2983](https://github.com/GetFelix/felix/issues/2983) is being built to
restore from them.

## See also

- [Durable storage](https://github.com/GetFelix/felix/blob/main/docs/durable-storage.md#restoring-to-a-backup-point)
  for why a live copy is safe to take and what `restore_to` does to a log.
- [Moving shards by hand](/deployment/moving-shards/) for
  `felixctl placement pause`, which keeps placement quiet while a point
  is taken.
