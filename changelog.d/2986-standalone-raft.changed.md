- A standalone broker (no `FELIX_NODE_ID`) with durable storage now runs each durable
  stream and cache shard as a Raft group of one voter, with no peer, internal listener or
  control plane, instead of the hybrid path (#2986). It is its own fleet, so every Raft
  feature is on from the start. `FELIX_SHARD_ENGINE=hybrid` still selects the hybrid
  (deprecated), and `FELIX_SHARD_ENGINE=raft` without durable storage is refused. Keyed
  cache watches are not served on Raft yet (#2792), so `compose.yaml` pins the hybrid.
  A standalone data directory the hybrid engine wrote is refused at startup, naming
  `FELIX_SHARD_ENGINE=hybrid` as the way to keep serving it; 0.8 does not migrate it.
