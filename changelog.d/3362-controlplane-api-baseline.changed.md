- The control plane's API has a 0.8 baseline, with no fallbacks for a broker or
  caller from before it (#3362). `FELIX_CONTROLPLANE_ACCEPT_BROKER_AUDIENCE`,
  `FELIX_CONTROLPLANE_LEGACY_UNSCOPED_GROUPS` and
  `FELIX_CONTROLPLANE_DELEGATE_UNBOUND_TOKENS` are retired: set, each is reported
  at start and changes nothing, and the secure behaviour it relaxed always
  applies. A registration answer no longer carries `storage_placement`, and a
  broker without durable storage no longer checks for it.
- A long-poll on `/v1/shard-assignments/changes` is woken by the store when a
  change is recorded, a Raft apply included, instead of re-reading the store
  every 50 ms (#3362).
