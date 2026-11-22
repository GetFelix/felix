- Raft groups can run on group threads (`FELIX_SHARD_RAFT_GROUP_THREADS`, off by
  default). Each group's openraft tasks and publish task share one
  single-threaded runtime, so the wake-ups inside a write are queue pushes
  instead of cross-thread wake-ups, and one wake-up of a thread serves every
  ready group on it (#2936).
