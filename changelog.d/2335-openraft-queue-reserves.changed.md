- openraft moves to upstream commit `30c947ef38` (#2335). An idle Raft member reserves 64-slot
  queues instead of room for 4,096 commands, and its resident memory no longer creeps up while it
  sits idle. The Raft memory test now runs in pull request CI. The control plane's state machine
  may now be handed several committed ranges in one `apply` call; entries still arrive in log
  order, once each.
