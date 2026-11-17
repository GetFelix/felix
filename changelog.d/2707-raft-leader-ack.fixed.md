- A `Leader` publish on a Raft shard whose leader lease does not hold is answered where a `Quorum` publish would be,
  not at its fsynced commit (#2707).
- A Raft shard's only voter holds its leader lease without an answer from anyone, so a single-node shard answers a
  `Leader` publish once it is on its own log, as the hybrid engine does. The lease ends the moment a membership with a
  second voter is appended. `FELIX_SHARD_RAFT_LEASE_MS=0` still turns it off (#2707).
