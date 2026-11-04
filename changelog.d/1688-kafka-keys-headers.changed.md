- `Broker::publish_records_idempotent` takes a new `headers` argument, each
  record's key and headers block (#1688). Pass `None` for the previous
  behaviour. `felix_kafka::Settings` gained `record_headers`, and
  `KafkaListenerConfig` the same field.
