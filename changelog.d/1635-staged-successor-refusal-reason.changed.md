- An RF 2 shard kept closed by a staged successor in the failover's fence ids
  now says so (#1635). The crashed leader's copy logs a warning naming the
  successor, counts `felix_broker_fence_refused_staged_successor_total`, and
  its refusal reaches the promoted leader's `felixctl inspect shard` reason,
  which names the successor and `felixctl placement abandon` as the way out.
  A fence refusal's detail now shows there in general. Replication itself is
  unchanged.
