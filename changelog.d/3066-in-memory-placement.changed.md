- The control plane places an in-memory (`durable: false`) stream's shard with
  a placement step of its own: one owner and no followers, whatever its
  `replication_factor`, kept while the owner is live and placed again in the
  same pass when it is lost, draining, internal or outside the stream's region,
  at a new generation as before. Balancing no longer moves these shards, and an
  operator move is one write answered with step `owner_replaced` (#3066).
