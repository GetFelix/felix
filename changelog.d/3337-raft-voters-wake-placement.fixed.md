- A one-replica Raft move finishes as soon as the leader reports the destination voting,
  instead of at the next placement pass a minute later (#3337). A leader's report of new
  voters, and a changed Raft copy id, now wake placement; repeats of the same voters do
  not.
