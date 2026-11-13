- A broker running Raft shards reads the `engine` markers of shards placed
  elsewhere before its routing pass takes the lock that stream deletes and
  group forming wait on, so a slow or failing read usually no longer holds
  them up (#2465).
