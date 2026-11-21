- A Raft shard's group can no longer form twice when placement moves before
  the group first forms (#2794). The control plane now refuses to change a
  shard's set once it has stored a target, until a leader of the group sends
  it a floor report, so a broker with a stale view of placement and one with
  a fresh view form the same set. Placement shows such a shard as waiting for
  its group to form. On from control-plane metadata level 45.
  Brokers send the floor of every group they lead again every 30 s, so an
  idle group gets its mark without a publish.
