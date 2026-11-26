- A plain publish to a Raft shard that a QUIC client waits on an ack for arms
  no timer of its own (#3225). Its quorum wait is timed out by the control
  stream's ack sweep, at the same `FELIX_PUBLISH_QUORUM_TIMEOUT_MS` and with
  the same `quorum_timeout` answer, and the commit wait behind its delivery
  has no deadline. Each was one timer armed and dropped per publish.
