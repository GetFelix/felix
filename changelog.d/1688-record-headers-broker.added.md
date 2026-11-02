- A single broker now accepts and delivers record headers (#1688). A client that
  offers `EXT_RECORD_HEADERS` in `Auth` is answered with it and can publish a
  key and headers with each record; a subscriber that offers it gets them back on
  live delivery, replay and after a restart. A durable stream stores them in
  storage format v8, so a log moves onto v8 only for the first record that has
  headers. Header names starting with `felix.` are reserved and refused. Every
  other client gets the same bytes as before. A clustered broker accepts them
  once its fleet finalizes `record_headers`. Consumer groups do not deliver headers. A retried idempotent
  batch is matched on its payloads only, so a re-send with changed headers is
  not stored with the new ones. `FELIX_RECORD_HEADERS=false` makes a single broker
  refuse every publish that carries headers, which keeps its logs at v7 and
  readable by older builds; turning it off later leaves stored headers readable.
  See `docs/protocol.md`.
