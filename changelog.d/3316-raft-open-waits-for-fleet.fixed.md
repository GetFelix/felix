- A Raft member opened right after a restart no longer fsyncs every commit on a broker
  configured for `periodic` or `none` until it next opens. The open pass now waits, for
  up to 10 s, until the broker's registration has said which fleet features are enabled,
  so the member reads `raft_written_commit` and `raft_alone_reform` as the fleet has them
  (#3316).
