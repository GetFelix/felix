- The metadata store's member file, for the control plane's move to the
  segment store (#2228). `store/member` holds the cluster id, the
  vote-withheld flag and the converted and started marks, under one checksum,
  and is replaced by one rename that is on disk before a change returns. A
  start is decided from the names in the data directory and that file, without
  reading `raft.redb`. A snapshot's leftover temporary is now deleted at open.
  The control plane does not use these files yet, so nothing changes for a
  running cluster. The format is in `docs/storage-format.md`.
