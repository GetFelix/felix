- A broker with Raft copies now records every run as relaxed, whatever `FELIX_DURABLE_FSYNC_MODE` says, so after a power cut it starts under a new storage id and its copies under new copy ids (#2423).
  A copy reports a record written before its fsync, so a copy that kept its id after losing that tail could elect a leader without a record already acknowledged on the written count. The old voter seat stays empty until membership changes exist.
  A Raft copy closed and reopened in the same process after a failed flush now rotates even when its poison marker could not be written (#2475).
