- A replicated shard whose majority needs two slow followers no longer stops
  acknowledging writes (#1895). The leader counted a follower toward the
  quorum mark only if it answered in the current replication pass, and two
  followers that kept answering in different passes were never counted
  together. An answer now keeps counting after its pass, for what it said the
  follower holds, until the leader sees that follower's copy change: a new
  storage identity or incarnation, a request to it that fails, or an exchange
  that has not yet found the same copy again.
