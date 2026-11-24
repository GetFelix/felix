- A sharded subscription recovers when its entry broker dies. Each lost shard now
  reconnects the client to another known broker and resubscribes through it,
  instead of retrying on the dead connection forever (#3061).
