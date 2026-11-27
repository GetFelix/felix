- A Raft stream or cache shard created while fewer brokers were live than its replication
  factor now forms once enough brokers join. Placement writes its full set in one step,
  since no group can have formed on a short set; before, the brokers waited for a full set
  and the control plane waited for their reports, so the shard stayed unformed for good
  (#3331). While it waits, placement logs the shard and the reason at info about once a
  minute.
  A leader that died or started draining before the group formed is left out of that write,
  and a live broker leads instead.
