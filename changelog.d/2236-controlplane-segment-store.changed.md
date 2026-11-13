- Breaking: the control plane keeps its Raft log, vote and snapshot on Felix's own
  storage engine, in `store/` under `FELIX_RAFT_DATA_DIR`, instead of `raft.redb`
  (#2230, #2236). The log is fsynced on commit, a log id is a term and an index, a
  member's started mark is on disk before its Raft node exists, and openraft is at
  0.10.0 (#2225). redb is gone from the control plane. There is no conversion: a member
  that finds a `raft.redb` from 0.7 or earlier refuses to start and changes nothing,
  so an existing group starts over on empty data directories (brokers too) and
  tenants, streams, caches and tokens are recreated. Peer routes move to
  `/internal/raft/v2`, so 0.7 and 0.8 members never form one group, and snapshots go
  to a peer as one streamed request. See the docs-site upgrades page, "Control plane
  metadata in 0.8".
