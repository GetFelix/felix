- A Raft broker syncs its vouched-unsynced marker once at startup instead of
  once per publish until the first sync lands. Each publish that acked at
  written started its own sync of the marker and its directory, and on APFS
  those full flushes stalled the log's writes: about 1.2 ms on the first 2%
  of publishes. Callers now wait for the sync already in flight.
