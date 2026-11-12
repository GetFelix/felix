- The control plane's metadata Raft group runs openraft 0.10.0, the workspace pin, still on redb
  (#2225). What members send each other and store is now Felix's own JSON, byte for byte what
  the releases on openraft 0.9 wrote, and snapshots still travel in chunks. So a group is
  upgraded one member at a time, and a member can be rolled back to v0.7.1 on the same data
  directory. A test runs the v0.7.1 binary and the current build in one group, sends a snapshot
  each way and rolls a member back.
