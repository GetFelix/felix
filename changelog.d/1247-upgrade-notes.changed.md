- **Upgrade notes for the follower-suspicion fixes (#1247).** The order check
  on finalizing fleet features runs on the control-plane member that serves the
  `finalize` request. During a rolling control-plane upgrade a member still on
  the previous release skips it, so finish upgrading every member before
  finalizing `majority_ack`, `lease_free_reads` or `fenced_caches`, or finalize
  them in order: `generation_start` first, then `majority_ack`, then the other
  two. A Raft control plane records a report's `by_round` only once every
  member is at metadata level 11; until then placement fails a leader over on
  its lease, not its followers' word. A Postgres control plane runs migration
  `0029` on start.
