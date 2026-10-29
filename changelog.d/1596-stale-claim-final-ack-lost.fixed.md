- A `stale_claim` refusal is no longer retried by the cluster client, and every
  client reports its retry class as `fatal`, so a consumer loop that logs it
  does not stall for the backoff (#1596). A publish written before the
  connection dropped, whose acknowledgement never arrived, now surfaces as an
  unknown outcome (`AckLost` in Rust, `OutcomeUnknownError` in Python and
  TypeScript, `FELIX_STATUS_OUTCOME_UNKNOWN` in C and C#) instead of a
  connection error. The wire format is unchanged.
