- `FELIX_SUB_QUEUE_MAX_BYTES` now counts only what waits in a subscriber's
  broker queue, and its default drops from 256 MiB to 64 MiB (#1195). Frames in
  the writer lanes count against a new per-connection bound,
  `FELIX_SUB_EGRESS_MAX_BYTES` (256 MiB), shared out equally among the
  connection's subscriptions, so one whose stream stalls stops at its share and
  never holds up the others. `FELIX_SUB_QUEUE_MAX_BYTES_TOTAL` still covers a
  batch until it is written. New gauge: `felix_sub_egress_bytes`.
