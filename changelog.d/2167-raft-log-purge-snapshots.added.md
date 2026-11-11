- The Raft log adapter over `DiskLog` now purges and keeps snapshots (#2167): `purge` with a
  persisted purged id (also past the log's end, as openraft asks at startup), and
  `SnapshotStore`, one file that holds a snapshot's id, membership and data. The adapter passes
  openraft's whole storage suite. Nothing runs on it yet.
