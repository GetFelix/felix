- The Kafka listener can keep record keys and headers (#1688). With
  `FELIX_KAFKA_RECORD_HEADERS=true`, a produced record's key and headers are
  stored with it when the broker also accepts record headers (a broker outside
  a cluster, unless `FELIX_RECORD_HEADERS=false`; a cluster once its fleet
  finalizes `record_headers`), and a fetch returns them, including those a
  Felix client published. The variable is off by default, because the first
  stored block moves the logs to storage format v8, which an older build
  refuses to open: enabling the Kafka listener alone never does that. Off, keys
  and headers are dropped and counted in `felix_kafka_produce_dropped_total`
  as before. A null header value is stored as an empty value, and a header name
  starting with `felix.` or an empty one is refused as `INVALID_RECORD` (the
  message reaches Produce v8 and later clients). Header bytes count toward the
  tenant's publish quota. Producer timestamps are still dropped. A record with
  no key or headers is fetched byte for byte as before.
