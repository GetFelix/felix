- The Raft history suite's liveness check no longer reports a serving shard as
  stuck. It counted a drained broker's member that never heard it was removed,
  and compared followers asked before the leader against the leader's commit
  index. `shard_inspect` now reports a Raft member's `voters`, and the check
  measures the majority against the leader's list.
