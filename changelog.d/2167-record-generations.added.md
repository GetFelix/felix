- New fleet feature `record_generations` (#2167), groundwork for per-shard
  Raft: it moves durable stream logs onto segment format v9, with every record
  stamped with a generation, and lets a shard save a committed vote. It exists
  but cannot be finalized yet: brokers do not report it and the control plane
  refuses it, until replication carries the generation that wrote each record.
  Nothing changes on disk, and a single broker stays on the older format.
