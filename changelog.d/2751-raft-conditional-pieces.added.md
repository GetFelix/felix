- A Raft shard can store a conditional batch of several records as
  conditional pieces (Raft entry tag 10), kept whole at the expected offsets
  or blanked whole on every member (#2751). Such a batch is still refused
  until the fleet finalizes `raft_conditional_batches`, which no build
  reports until every Raft read path applies the whole-batch rule.
