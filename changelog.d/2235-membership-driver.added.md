- Per-shard Raft (behind the `raft` cargo feature, which no release enables): a group's leader
  now makes its membership match a target it is told (`GroupRuntime::retarget`): it adds new
  members as learners, changes the voters once they have caught up, and hands leadership over
  first when the target leaves it out (#2235). It never writes a membership without itself and
  never keeps a voter that left as a learner (#2498). `GroupRuntime::target_members` names a
  target's members by the copy each broker holds. The control plane does not send targets
  yet. Progress is in `GroupStatus::membership` and four `felix_broker_raft_membership_*`
  metrics.
