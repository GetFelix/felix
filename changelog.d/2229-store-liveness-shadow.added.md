- Store liveness in shadow mode, the first runtime piece of per-shard Raft (#2229). Brokers that share a shard
  exchange a heartbeat tied to a synced disk probe and carrying a store epoch, and record what they would decide.
  Nothing acts on it. It is compiled only with the `raft` cargo feature, which no release enables, and runs only
  with `FELIX_STORE_LIVENESS=shadow`. The internal protocol gains the `StoreLiveness` kind and the
  `STORE_LIVENESS` capability, which no release offers.
