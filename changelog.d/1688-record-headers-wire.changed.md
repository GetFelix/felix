- The wire types changed in ways that break code building or matching them
  (#1688). `PublishBatch`, `AckedPublishBatch`'s batch, `EventBatch` and
  `SharedEventBatch` gain a `record_headers` field, `EventBatchMeta` gains
  `record_headers`, and `Message::Auth` and `Message::AuthOk` gain
  `client_flags_ext` and `server_flags_ext`, so struct literals and exhaustive
  patterns need the new fields. `Error` gains `UnknownExtension` and
  `InvalidRecordHeaders`. `has_unknown_flags` and `Frame::decode` now accept
  `0x8000` (`FLAG_EXTENDED`); with all sixteen flag bits defined, no frame is
  refused for its flags alone, and the body decoders refuse the bit where it has
  no meaning. The broker still refuses it.
