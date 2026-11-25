- `felix-cluster failover` and `felix-cluster consistency` run on the Raft shard
  engine, which `felix-cluster` starts its brokers on (#2985). `failover` waited
  on a gauge only the older engine reports. `consistency` now shows what a
  `Leader` stream loses on Raft: the records its leader acknowledged alone
  before it was lost.
