- The Kafka listener keeps record keys and headers (#1688). A produced record's
  key and headers are stored with it when the broker accepts record headers (a
  broker outside a cluster, unless `FELIX_RECORD_HEADERS=false`; a cluster once
  its fleet finalizes `record_headers`), and a fetch returns them, including
  those a Felix client published. A null header value is stored as an empty
  value, and a header name starting with `felix.` or an empty one is refused as
  `INVALID_RECORD`. Header bytes count toward the tenant's publish quota. A
  broker that does not accept headers still drops them and counts them in
  `felix_kafka_produce_dropped_total`, so an old fleet keeps taking Kafka
  produces. Producer timestamps are still dropped. A record with no key or
  headers is fetched byte for byte as before.
- `Broker::publish_records_idempotent` takes each record's key and headers
  block.
