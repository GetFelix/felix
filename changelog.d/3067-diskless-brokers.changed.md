- A broker with a node id and no `FELIX_DURABLE_STORAGE_DIR` now joins a Raft
  cluster instead of refusing to start. It runs no Raft group and registers
  the node label `felix.dev/node-storage=none`. The control plane gives it only
  in-memory stream shards: no durable stream's or cache's shard is placed or
  moved there, one already there is moved off, and an operator move there is
  refused with `no_durable_storage`. The registration needs every
  control-plane member at metadata level 54, and the broker refuses a control
  plane that predates it. A cache request through it goes to the cache's Raft
  group (#3067).
