- A Raft shard member that cannot reach the others no longer raises its term at every election timeout, and no
  longer deposes the leader when it reconnects: shard groups now run openraft's pre-vote, and a member that still
  hears a live leader refuses a pre-vote even in an idle group (#2630).
