- The C ABI (`crates/sdk/felix-capi`) publishes everything the Rust cluster client does apart from
  idempotent and conditional writes (#1493). `felix_client_publish` takes an optional routing key
  and a `FelixDelivery` (`FELIX_DELIVERY_AT_LEAST_ONCE` resends after a reconnect, keyed or not).
  `felix_client_publish_batch` sends a batch to one shard, unkeyed or by key,
  `felix_client_publish_on_shard` to a shard the caller names, and `felix_client_publish_records`
  takes a key per record and sends each shard its share. Batch payloads are passed end to end in
  one buffer with an array of lengths, and the callback gets a `FelixRecordResult` per record with
  its status and log offset.
