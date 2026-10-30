- An RF 2 shard kept closed because the failover's fence ids name a copy
  beside the set of two now says so (#1635). The crashed leader's copy logs a
  warning naming the extra id (a staged or joining successor the failover left
  out), counts
  `felix_broker_fence_refused_ids_beyond_two_total`, and its refusal reaches
  the promoted leader's `felixctl inspect shard` reason.
  A fence refusal's detail now shows there in general. Replication itself is
  unchanged.
