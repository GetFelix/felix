- A standalone broker (no `FELIX_NODE_ID`) with durable storage now runs each durable
  stream and cache shard as a Raft group of one voter, with no peer, internal listener or
  control plane, instead of the hybrid path (#2986). It is its own fleet, so every Raft
  feature is on from the start. `FELIX_SHARD_ENGINE=hybrid` still selects the hybrid
  (deprecated), and `FELIX_SHARD_ENGINE=raft` without durable storage is refused. Keyed
  cache watches are not served on Raft yet (#2792), so `compose.yaml` pins the hybrid.
  A standalone data directory the hybrid engine wrote is refused at startup, naming
  `FELIX_SHARD_ENGINE=hybrid` as the way to keep serving it; 0.8 does not migrate it.
  After a machine crash under `Periodic` or `None` fsync, which rotates the storage id, each
  of a standalone broker's Raft groups re-forms itself from its own copy and keeps serving:
  it keeps every record that reached the disk and gives out no offset twice. A clean stop,
  an `OnCommit` crash or an operator's rotation does not re-form.
