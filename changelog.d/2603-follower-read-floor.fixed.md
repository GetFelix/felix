- A member of a stream shard on Raft no longer serves a resume, cursor
  subscribe, tail subscribe, history page, stream read or Kafka fetch below a
  head trim the leader already answered (#2603). A follower first asks the
  leader for a read index and waits until it has applied it, a plain tail
  subscribe without event offsets included; a leader takes its commit index
  while its leader lease holds and runs its own read-index round otherwise, so
  a leader cut off from a successor that trimmed refuses the read instead of
  serving it. Either refuses the read as retryable
  if it cannot. The read index is a new Raft peer message, so it waits for the
  fleet feature `raft_follower_read`; until then a follower redirects such a
  subscribe to the leader. A new cluster starts with it finalized.
