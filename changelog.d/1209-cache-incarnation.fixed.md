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
