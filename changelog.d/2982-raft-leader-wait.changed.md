- `shard_inspect` answers carry the broker's Raft member (`raft`: role, term, leader, last index,
  commit index) when it holds one. The cluster test harness uses it to wait for a shard's own
  Raft leader and a caught-up majority (#2982).
