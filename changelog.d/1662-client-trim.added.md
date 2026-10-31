- The Rust client and `felixctl` send the conditional head trim, step 7
  of #1662. `Client::trim_before` and `Connection::trim_before` take a
  shard, the new floor, the expected next offset and optional headers. A stale
  offset comes back as `ConditionalTrim::Refused { tail }` and is not retried;
  a timed-out trim is a `BrokerError` with `OutcomeUnknown`; a broker without
  `FEATURE_HEAD_TRIM` is refused locally with `TrimError::Unsupported`.
  `SubscribeCursorError` gains `trim_headers`, so it is no longer `Copy`.
  `felixctl stream trim` prints where the trim landed and where reads start.
  A cluster test shows a trim and its floor surviving the leader.
