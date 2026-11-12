- The control plane's metadata group can be built on the segment store
  (#2230). No release does: the build needs
  `RUSTFLAGS="--cfg felix_segment_store"`, and without it the group runs on
  redb exactly as before. With it the Raft log, vote and committed index
  live in a `DiskLog` fsynced on commit, the snapshot in one file, and a
  log id is a term and an index. Every peer route moves from
  `/internal/raft` to `/internal/raft/v2`, so a member of this build and a
  member on redb answer each other with a 404 and never exchange Raft
  messages. A snapshot travels in one request. The member's started mark is
  on disk before its Raft node exists, and a data directory that holds a
  `raft.redb` with no converted store beside it is refused with nothing
  written. CI runs the control plane's suites on both builds
  (`task segment-store:check`).
- `felix_meta_raft_term` shows a term of 2^62 or more with 2^62 taken off,
  the range a converted store will use. No store reaches it today.
- `task lint` checks that `felix-broker` and the broker binary do not depend
  on openraft and that openraft's types stay in `felix-storage`'s `raft_log`
  and the control plane's `src/raft/`
  (`scripts/check-openraft-containment.sh`).
- `RaftHandle::enter_group` is now `async`.
