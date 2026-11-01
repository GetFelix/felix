- The control plane keeps a cut-over's new replica set pending under
  `replica_election` (#1009). The old set stays the control plane's until the
  leader reports its generation's start record acknowledged on a majority of
  both, and a failover, a cancel, a consistency change or
  `FELIX_SHARD_PENDING_SET_TIMEOUT_MS` (ten minutes) drops it. Nothing changes until
  `replica_election` is finalized, which no broker supports yet. Postgres
  runs migration `0038`; under Raft it takes metadata level 21.
