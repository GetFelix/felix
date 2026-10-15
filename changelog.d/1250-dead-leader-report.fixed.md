- A dead leader's last replica report no longer expires while placement or the
  control plane is slow, which left the shard unplaceable until an operator
  abandoned it (#1250). The store now stamps every node a report names with its
  node incarnation as it accepts the report. Placement counts a node from a
  report only while that is still its incarnation, so a follower that
  registered again since, and may have lost its log, is not promoted or ranked
  on it. Once the leader is down and the assignment is still at the report's
  generation, a stamped report's age is measured to the leader's last heartbeat
  instead of now. A live leader's report still expires. Postgres migration
  `0030_replica_report_follower_incarnations`; under Raft, metadata level 12.
  Unstamped reports are read as before.
