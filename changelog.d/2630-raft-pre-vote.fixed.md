- A Raft shard member that cannot reach the others no longer raises its term at every election timeout, and no
  longer deposes the leader when it reconnects: shard groups now run openraft's pre-vote (#2630).
