- Per-shard Raft (behind the `raft` cargo feature, which no release enables):
  `GroupRuntime::retarget` and `RaftEngine::retarget` take the term the target is told for. A
  group's leader acts only on a target told for the term it leads, so a new leader waits to be
  told again instead of acting on one an earlier leader held (#2235, #2591).
