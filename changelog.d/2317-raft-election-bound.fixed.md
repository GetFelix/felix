- A broker bounds how many of its Raft groups are in an election at once (`RuntimeConfig::max_elections_at_once`,
  `FELIX_SHARD_RAFT_ELECTIONS_AT_ONCE`, 64 by default, `0` for no bound), so thousands of groups that lose their leader
  together, or are formed together, settle instead of re-running elections whose vote saves outlast the timeout (#2317).
  The limit falls while vote saves are several times slower than one with the disk to itself, never below 8, and climbs
  back each tick without a slow save. Groups that took writes go first. New gauges
  `felix_broker_raft_elections_in_flight` and `felix_broker_raft_elections_waiting`.
