- On a Raft cluster, placement and operator moves treat a cache shard as a Raft
  group: a drain or rebalance renames its leader to another member instead of
  staging a hybrid move the group never acts on (#2834).
