- On a cluster with `shard_engine: raft`, control-plane placement no longer runs hybrid moves or failover on Raft shards (#2650).
  A balance pass, a drain or an operator move now names another member of the shard's set as placement leader in one write, so a drain of a broker that leads Raft shards finishes.
  A dead leader is renamed to a live member instead of being reported unplaceable, and an operator move to a broker outside the set is refused with `not_a_member`.
  A placement write that keeps a shard's set, such as a leader move, keeps its Raft target and copy ids.
