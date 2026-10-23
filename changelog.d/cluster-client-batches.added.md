- `ClusterClient` publishes batches owner-direct: `publish_batch`, `publish_batch_keyed`, and
  `publish_records`/`publish_records_at_least_once`, which split records by shard and send each
  shard's ordered batch to its leader, reporting failures per shard in a `BatchReport`. Before, only
  single records were leader-direct, so batched publishing on a cluster was forwarded by a
  non-leader. `felix-loadgen ingest --cluster` uses it, and a loadgen publisher now survives a
  broker loss and reports `reconnects`, `failed_sends` and per-shard acked counts.
- `ClusterClient` now looks up shard leaders from the cluster (`FEATURE_SHARD_OWNERS`) on a stream's
  first publish, every few seconds, and after a publish finds one wrong, so the first batch and
  publishes that ask for no ack go leader-direct. Before, a leader was learned only from the ack of
  a forwarded batch, so fire-and-forget publishing was forwarded for as long as it ran. A cached
  leader learned under no stream width is no longer trusted for a keyed publish.
