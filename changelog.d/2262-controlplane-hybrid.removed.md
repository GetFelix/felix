- Breaking: the control plane no longer serves `GET` or `POST
  /v1/shard-assignments/confirmed` (#2262). Only the hybrid's brokers asked it, before
  they voted for a leader. A Raft group elects its own leader from its own log.
