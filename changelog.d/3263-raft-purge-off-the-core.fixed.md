- A Raft leader no longer stops heartbeating while it purges its log behind
  a slow fsync. openraft runs the purge on the member's core, often right
  after a follower goes away, and the purge waited for the log's flush lock;
  on a slow disk the leader lost its lease and publishes timed out. The
  purge now writes its purged id and returns, and the segments below it are
  unlinked behind it; a crash part way is finished at the next open. A call
  into the log that outlasts a heartbeat is counted in
  `felix_storage_raft_log_slow_calls_total` and logged (#3263).
