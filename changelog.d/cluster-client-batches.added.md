- `ClusterClient` publishes batches owner-direct: `publish_batch`, `publish_batch_keyed`, and
  `publish_records`/`publish_records_at_least_once`, which split records by shard and send each
  shard's ordered batch to its leader, reporting failures per shard in a `BatchReport`. Before, only
  single records were leader-direct, so batched publishing on a cluster was forwarded by a
  non-leader. `felix-loadgen ingest --cluster` uses it, and a loadgen publisher now survives a
  broker loss and reports `reconnects`, `failed_sends` and per-shard acked counts.
