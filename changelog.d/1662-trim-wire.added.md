- Clients can send a conditional head trim, the fifth piece of #1662.
  `trim_before` names a shard, the floor and the expected tail, and is
  answered with `trim_ok` (where the record landed and the new floor) or
  `publish_refused` with `offset_mismatch`. It is negotiated as
  `FEATURE_HEAD_TRIM`, the fifth bit of the extended feature word. Two new
  error codes, `feature_not_enabled` (number 18) and `stream_has_commits`
  (19), and the reasons `trim_past_tail`, `not_durable` and `trim_timed_out`
  name each refusal. `subscribe_cursor_error` carries `trim_headers` below a
  trim's floor, only to a client that offered the bit. A new action,
  `stream.trim`, allows it, and `stream.manage` grants it too; the control
  plane mints `stream.trim` into broker tokens only once the fleet has
  finalized `head_trim`. The first trim writes a `head-trimmed` marker in the
  data directory, and a broker holding it refuses to join a fleet that has
  not finalized `head_trim`.
