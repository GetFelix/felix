- A cache shard's cache and counters can run as one Raft group (#2238). The leader stamps each
  write with the log's time and every member applies it the same way, a retried write gets its
  first answer, a read waits for the read index to be applied, and an expired key is deleted by
  a committed entry before it reads absent. Snapshots carry the state, so a member that installs
  one answers like one that replayed the log. Behind the `raft` feature; no shard is routed to it.
