- Per-shard Raft (behind the `raft` cargo feature, which no release enables): a membership
  entry now carries the version of the placement target it was made from (#2235). A stored
  membership is layout 2, and layout 1 still reads. A Raft message between brokers is layout 3,
  so brokers built before and after this change refuse each other's Raft messages by name.
