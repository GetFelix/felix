- The Raft log adapter over `DiskLog` now writes (#2167): `append` with openraft's
  `IOFlushed` callback, `truncate_after` and `save_vote`. A log opened with
  `LogConfig::raft_log` is segment format v9 from its first segment, takes
  membership records and refuses retention bounds. Nothing runs on it yet.
