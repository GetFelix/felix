- A follower of a stream shard on Raft no longer serves a resume, cursor
  subscribe or history page below a head trim the leader already answered
  (#2603). It first asks the leader for a read index and waits until it has
  applied it, and refuses the read as retryable if it cannot. The read index is
  a new Raft peer message, so it waits for the fleet feature
  `raft_follower_read`; until then a follower redirects such a subscribe to the
  leader. A new cluster starts with it finalized.
