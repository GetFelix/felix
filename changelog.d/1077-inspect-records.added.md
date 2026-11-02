- `felix-broker inspect records` (#1077). Decodes the records around an offset of
  one shard in a data directory, offline and read-only: the offset, the time the
  record carries, its kind, payload size, where it sits on disk and whether it
  verifies, with payload bytes only on request (`--payload`, cut at 64 KiB each).
  It seeks with the sparse index, which it checks rather than trusts, and a record
  that fails its checksum ends the read instead of failing it, so the rows before
  the damage stay readable. Use it after `inspect segments` names a position.
