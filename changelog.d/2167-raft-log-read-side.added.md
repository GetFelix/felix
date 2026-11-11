- `felix-storage` gains a `raft` cargo feature with the read side of the Raft
  log adapter (#2167): a `DiskLog` read as an openraft 0.10.0 log, one record
  per entry, with the term taken from the generation in the record header.
  Nothing uses it yet. openraft is pinned to `=0.10.0` for it.
