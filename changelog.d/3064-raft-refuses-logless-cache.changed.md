- A broker that runs its shards on Raft and has `FELIX_DURABLE_STORAGE_DIR` set refuses to start
  if its cache store keeps no logs, naming the setting (#3064). A Raft cluster has no in-memory
  cache; a broker with no disk is not affected.
