- A metadata Raft leader no longer judges a join, whether to feed a member,
  or the level it settles a proposal at, by a join floor older than its
  committed log (#2878). Before, a newly elected leader that had not yet
  applied the entry raising the floor, or any leader reading a floor raised
  earlier in the same term, could add a member below it, send it entries it
  cannot apply, or write a command at a level below what the log needs. A
  follower whose own lagging floor would decide the level now asks the leader
  for it, so it no longer strips storage ids or learners the group can store.
  A leader that cannot confirm its term in time now answers a retryable 503
  `unavailable` instead of settling on a stale level.
