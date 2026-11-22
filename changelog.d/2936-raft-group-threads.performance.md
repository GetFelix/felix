- Raft groups run on group threads, one per four cores and at most 8 (none
  under four cores). Each group's openraft tasks and publish task share one
  single-threaded runtime, so the wake-ups inside a write are queue pushes
  instead of cross-thread wake-ups, and one wake-up of a thread serves every
  ready group on it. Raft CPU per message falls 12-33% (#2936).
