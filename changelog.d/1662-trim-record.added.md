- Storage has a trim record, the first piece of the conditional head trim
  (#1662). It is a new record kind that holds a floor and the caller's
  headers, and it needs segment format v7. A normal read leaves trim records
  out, and `read_range_with_trims` returns them. Nothing writes one yet, so
  a log stays on v6 or older and its bytes are unchanged. See
  `docs/storage-format.md`, "Trim records".
