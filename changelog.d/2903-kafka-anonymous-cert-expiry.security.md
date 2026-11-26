- A Kafka connection under `FELIX_KAFKA_ANONYMOUS_TENANT` whose client presented a
  certificate now closes at that certificate's `notAfter`, and a request it sends
  after that is refused, as on an authenticated connection. A certificate that has
  already ended or cannot be read is refused. A connection with no certificate is
  unchanged (#2903).
