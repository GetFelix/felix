- Per-shard Raft (behind the `raft` cargo feature, which no release enables): a broker now tells
  the Raft member it leads the target the shard feed carries, once per feed pass, and only from a
  feed answer that started after the term began and reaches the feed head a heartbeat sent after
  it brought. A new leader is told again for its own term, so it never acts on a target an
  earlier leader held (#2235, #2591). The `felix_broker_raft_shards_off_placement` warning now
  says the group is converging to its target.
