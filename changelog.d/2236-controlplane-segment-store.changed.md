- Breaking: 0.8 does not upgrade a 0.7 control plane's metadata. To move to 0.8,
  stop the cluster, start the control plane and every broker on 0.8 with empty data
  directories, and recreate tenants, namespaces, streams, caches and tokens. The
  control plane keeps its Raft log, vote and snapshot on Felix's own
  storage engine, in `store/` under `FELIX_RAFT_DATA_DIR`, instead of `raft.redb`
  (#2230, #2236). The log is fsynced on commit, a log id is a term and an index, a
  member's started mark is on disk before its Raft node exists, and openraft is at
  0.10.0 (#2225). redb is gone from the control plane. There is no conversion tool: a
  member that finds a `raft.redb` from 0.7 or earlier refuses to start and changes
  nothing. Peer routes move to
  `/internal/raft/v2`, so 0.7 and 0.8 members never form one group, and snapshots go
  to a peer as one streamed request. See the docs-site upgrades page, "Control plane
  metadata in 0.8".
