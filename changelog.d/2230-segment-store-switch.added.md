- `felix_meta_raft_term` and the status route's term show a term of 2^62 or more with 2^62
  taken off, the range a store converted from redb would use (#2230). No store reaches it
  today.
- `task lint` checks that `felix-broker` and the broker binary do not depend on openraft and
  that openraft's types stay in `felix-storage`'s `raft_log` and the control plane's
  `src/raft/` (`scripts/check-openraft-containment.sh`).
- `RaftHandle::enter_group` is now `async`.
