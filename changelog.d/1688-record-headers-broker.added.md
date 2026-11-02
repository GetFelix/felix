- A single broker now accepts and delivers record headers (#1688). A client that
  offers `EXT_RECORD_HEADERS` in `Auth` is answered with it and can publish a
  key and headers with each record; a subscriber that offers it gets them back on
  live delivery, replay and after a restart. A durable stream stores them in
  storage format v8, so a log moves onto v8 only for the first record that has
  headers. Header names starting with `felix.` are reserved and refused. Every
  other client gets the same bytes as before. A clustered broker does not accept
  headers yet, because replication does not ship them, and a forward that carries
  them is refused. Consumer groups do not deliver headers. See
  `docs/protocol.md`.
