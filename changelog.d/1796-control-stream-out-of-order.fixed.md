- A broker answers fences, tail fetches, pre-votes and pings on a peer
  connection's control stream as each one finishes, instead of one at a time
  (#1796). During a burst of shard creates, the fences that confirm cache reads
  and the pings followers send their leaders no longer queue behind the new
  shards' promotion fences, where a long enough queue read as a dead leader.
  Answers are matched by correlation id, so older peers need no change.
