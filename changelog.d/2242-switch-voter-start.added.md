- A Raft member can start on a shard directory the hybrid wrote, from the base
  a switch records: the copy checks the base and its digest, cuts what lies
  above it, and starts with the log purged through it and a vote at its term,
  so the first election is above every generation the hybrid used and no
  record below the base is rewritten or read as an entry (#2242). Nothing
  seals a shard yet, so no shard switches.
