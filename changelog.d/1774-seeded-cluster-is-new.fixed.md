- A cluster whose streams or caches are created before its first broker registers, as a
  seed job does, is new again: the first registration finalizes the new-cluster fleet
  features, so atomic commits work and Raft members open with `raft_written_commit` on
  instead of fsyncing every commit until they restart (#1774). A cluster that placed a
  shard on a broker, or was restored from a backup, still leaves finalizing to the
  operator.
