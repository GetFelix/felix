- The C ABI (`crates/sdk/felix-capi`) runs idempotent producers (#1493): `felix_client_idempotent_producer` makes a
  `FelixProducer`, and `felix_producer_publish` and `_publish_batches` send batches that land once however many times
  they are sent, `_abandon_in_doubt` gives up on a stream's batches in doubt, `_id` and `_id_for` read the producer
  id, and `_close` drains and stops it. A producer used after `_close` reports `FELIX_STATUS_CLOSED`.
