- Per-shard Raft (behind the `raft` cargo feature, which no release enables): a broker now tells
  the Raft member it leads the target the shard feed carries, once per feed pass, and only from a
  feed answer that started after the term began and reaches the feed head a heartbeat sent after
  it brought. A new leader is told again for its own term, so it never acts on a target an
  earlier leader held (#2235, #2591). A target from a placement incarnation below the one the
  heartbeat reports, from a control-plane member that has not applied a restore, is neither told
  nor acted on, and a fresh view with no target tells the leader so, which lets placement move it. The `felix_broker_raft_shards_off_placement` warning now
  says the group is converging to its target.
