- Replication and forwarding carry record headers (#1688). A leader ships each
  record's key and headers with it to a follower that offers the new
  `RECORD_HEADERS` peer capability, so the follower's log holds the same
  records, a promoted replica delivers the same headers, and the promotion
  catch-up fetch brings them back too. The batch checksum covers them. A forward
  carries them to a shard owner that offers the capability. A follower from an
  older build is held at the first record with headers instead of being sent it
  bare (`headers_unsupported` in `felix_broker_replication_shipped_total`). A
  cluster accepts record headers once its fleet finalizes `record_headers`,
  which a new cluster does at its first registration. New peer message kinds 48
  to 50; a batch without headers is byte for byte what it was. See
  `docs/internal-protocol.md` and `docs/replication-design.md`.
