- Giving up a follower replacement on a durable `Quorum` stream or `Quorum`
  cache no longer risks an acknowledged record (#1628). When dropping the copy
  being added leaves an even set, an RF 2 set being the common case, a record
  could have been acknowledged on the leader and that copy alone, and the set
  left would hold it on the leader only. Placement now fences the leader first
  and drops the copy only once the drained leader's report shows every other
  follower holding its whole log, the same rule as a fenced seat. This covers a
  timeout, a newcomer that halts or goes down, a drain that is cancelled or a
  lost copy that returns, and an operator's cancel. Until that report arrives
  the copy stays, and writes pause for one fence as they do for a move.
