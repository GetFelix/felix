- An in-memory `Quorum` stream no longer serves without its lease once the fleet
  finalizes `majority_ack` and `lease_free_reads` (#3069). It has no log on its
  followers, so an owner cut off from the control plane could keep acknowledging
  beside the broker the shard was placed on next. Lease-free serving and sessions
  now apply only to shards that keep a log.
