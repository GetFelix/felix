- A Raft leader no longer stops heartbeating while it purges its log behind
  a slow fsync. openraft runs the purge on the member's core, often right
  after a follower goes away, and the purge waited for the log's flush lock;
  on a slow disk the leader lost its lease and publishes timed out. The
  purge now writes its purged id and returns, and the segments below it are
  unlinked behind it. Segments a crash leaves below the purged id are not
  entries, are ignored at open and are removed by the next purge. An unlink
  that fails stops the log, as the failed purge did before. A call
  into the log that outlasts a heartbeat is counted in
  `felix_storage_raft_log_slow_calls_total` and logged (#3263).
