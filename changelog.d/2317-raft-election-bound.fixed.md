- A broker bounds how many of its Raft groups are in an election at once (`RuntimeConfig::max_elections_at_once`,
  `FELIX_SHARD_RAFT_ELECTIONS_AT_ONCE`, 64 by default, `0` for no bound), so thousands of groups that lose their leader
  together, or are formed together, settle instead of re-running elections whose vote saves outlast the timeout (#2317).
  The limit falls while elections are losing and vote saves are several times slower than one with the disk to itself,
  never below 8, and climbs back by the elections that won, at once while elections win and one place a second while they lose. No member is let in while the vote saves queued on the broker would take over a quarter of the election timeout to drain. Groups that took writes go first. A
  leader whose transfer is not taken stands again without waiting for a place. New gauges
  `felix_broker_raft_elections_in_flight` and `felix_broker_raft_elections_waiting`. The limit may rise to four times the bound while elections win and the save queue drains, and returns to the bound on losses.
