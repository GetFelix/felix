- A replicated shard no longer holds a new record back from its healthy
  followers while it waits on a dead one (#1886). A pass with no majority yet
  waited on every follower it had shipped to, and only gave up early when some
  follower was busy with an earlier pass. A level follower that had been sent
  nothing could not make the majority, so a `Quorum` publish waited out the
  dead peer's request. Now a record appended to the shard ends that wait once
  a follower that counts has answered.
