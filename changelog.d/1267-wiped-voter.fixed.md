- A broker whose data directory was wiped no longer answers a fence for a shard
  its routing view does not have yet, or a leader other than the one the view
  names at that generation (#1267). The wipe took the promises it had given, so a
  deposed leader could count it toward a majority its successor had already
  fenced, and acknowledge a write the successor never held.
