- An in-memory stream with a replication factor above 1 no longer keeps an
  empty log on its leader or its followers (#3071, #3158). The broker refuses
  to open the records log of a stream registered in memory, and replication
  reports the shard caught up without one, so moves and drains still finish.
  Its consumer groups' committed positions stay on the leader and do not
  survive a failover.
