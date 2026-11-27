- A Raft voter that died after its shard's set changed, before it reported its copy again,
  no longer blocks every later placement step for the shard. Placement records the dead
  voter's copy from the voters the group reported, so the target for the set is stored and
  the voter is then replaced (#3313).
