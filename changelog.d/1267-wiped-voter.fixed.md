- A replica holding no fence promise on disk, such as one on a wiped data
  directory, no longer answers a fence for a shard its routing view does not have
  yet, or a leader other than the one the view names at that generation (#1267).
  A wipe took the promises it had given, so a deposed leader could count it toward
  a majority its successor had already fenced, and acknowledge a write the
  successor never held. A new shard's first fence may now wait for a follower's
  assignment watch.
