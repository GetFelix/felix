- A cache deleted and created again under the same name no longer serves or
  replicates the deleted cache's entries and counters once the fleet finalizes
  the new `cache_incarnations` feature (#1209). Each created cache then gets
  an `incarnation`, carried in the catalog and its shard assignments, and
  brokers keep each incarnation's cache and counter logs in directories named
  with it. Every cache delete after the finalize, cascades included, records a
  tombstone served in `GET /v1/caches/snapshot` as `tombstones` and on the
  `Deleted` change as `deleted_through`, and brokers delete a cache's data,
  from both stores, only on one of those or on an operator's removal, so a
  broker that was down through the delete still deletes it. Cache and counter
  replication requests name the incarnation and a follower holding another
  refuses them. Stale registrations are refused and counted in
  `felix_broker_cache_incarnation_refused_total`. Finalizing needs every Raft
  member at metadata version 14; Postgres gets migration 0032. Existing caches
  stay at incarnation 0 with their directories and data. A broker without
  durable storage drops a deleted cache's entries.
- `Broker::remove_cache` is now an operator's removal and deletes the cache's
  data; the catalog path uses `Broker::remove_cache_from_catalog`.
- Raft members on different builds no longer diverge when a finalize of
  `cache_incarnations` or `stream_incarnations` is committed through a member
  that predates it. Create and delete entries now carry an `incarnations`
  flag (metadata level 14), set only once every member is at level 14, and
  apply decides incarnations and tombstones from the entry alone (see
  `docs/metadata-raft-design.md`).
- Rolling a `v0.6.0-preview.7` Raft control plane to this release no longer
  splits it if `stream_incarnations` is finalized through a preview.7 member
  (which accepts the unknown finalize): a stream create from an older
  proposer that names no incarnation is stored at 0, and its delete records no
  tombstone, on every member. Clusters that ran an unreleased build at
  metadata level 10 to 13 and logged such an entry replay it differently now
  and must rebuild those members from a snapshot.
