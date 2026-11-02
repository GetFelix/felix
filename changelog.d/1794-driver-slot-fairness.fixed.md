- A leader no longer starves shards when more than 16 have work (#1794). Under
  a steady stream of appends the same 16 shards kept the replication pass
  slots, and a shard with one write could wait until its `Quorum` publish timed
  out with both followers idle. Shards now take turns: a finished pass goes
  behind the shards already waiting, shards with new writes or a write waiting
  on the mark go first, and fences have their own 16 slots. An append now wakes
  a pass for its own shard only, instead of every shard the broker leads, which
  also cuts the driver's work on a broker leading many idle shards. Only the
  first append to a shard since the driver last looked wakes it, so a busy
  shard's publishes no longer contend on one shared wake-up per append. New
  metrics show the queue (#1797): `felix_broker_replication_scans_total{wake}`,
  `felix_broker_replication_passes_waiting` and
  `felix_broker_replication_slot_wait_seconds{queue}`.
