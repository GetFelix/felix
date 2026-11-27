- A Raft broker opens each new shard's copy (its directory lock, copy file, rotation and
  log) on the blocking pool instead of a runtime worker. During a bulk create those
  fsyncs ran one shard after another on a worker and stalled the tasks behind them, so
  lease-cache reads on existing shards took over 2 s (#3032).
