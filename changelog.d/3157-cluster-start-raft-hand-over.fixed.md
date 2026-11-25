- The cluster test harness's `Cluster::start` now waits until every Raft shard
  is led by the broker placement names (#3157). Before, a test could read a
  shard's owner while the broker that formed the group still led it, and
  watch the leader move or its first publish be refused. A test of the
  hand-over itself sets `ClusterConfig::pending_raft_hand_overs`.
