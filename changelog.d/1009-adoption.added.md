- The control plane adopts a leader that a shard's replicas elected under
  `replica_election` (#1009). A broker's replica report can name its election
  (`elected`); on the set the control plane records the broker becomes the
  leader at the elected generation, on any other set only the generation is
  taken, and every later generation is above it. Promotion on the followers'
  word waits `FELIX_SHARD_ELECTION_HOLD_MS` (10 s) under the feature. Nothing
  changes until `replica_election` is finalized, which no broker supports
  yet. Under Raft adoption takes metadata level 21.
