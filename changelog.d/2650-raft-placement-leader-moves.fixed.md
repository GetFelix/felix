- On a cluster with `shard_engine: raft`, control-plane placement no longer runs hybrid moves or failover on Raft shards (#2650).
  A balance pass, a drain or an operator move now names another member of the shard's set as leader in one write, so a drain of a broker that leads Raft shards finishes.
  A dead leader's shard is no longer reported unplaceable, and an operator move to a broker outside the set is refused with `not_a_member`.
