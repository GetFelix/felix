- The Raft log adapter no longer spawns a task per append. One task per log waits for the
  group commit and answers every append that was waiting on it, in order (#2233).
- The Raft log adapter keeps the last 4 MiB of appended entries in memory, so a replication
  read of fresh entries does not read a segment. A truncation, purge or reset turns the cache
  off, so it never returns an entry the log no longer holds (#2233).
