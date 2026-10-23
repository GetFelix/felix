- The load generator's `ingest` scenario now stops issuing at the deadline, waits for outstanding
  acks, and calls `finish()` on every publisher instead of dropping the connections. Publishes
  still queued at the broker used to be cancelled, so published bytes exceeded accepted bytes by a
  few batches per connection. The drain is reported as `finish_ms` in `LOADGEN_JSON` (with
  `finish_failed`) and is excluded from `duration_s` and the throughput figures. Acked runs no
  longer remove in-flight sends from `published` at the deadline; only sends still unanswered
  after the 60 s drain bound count as `cut_off_records`.
