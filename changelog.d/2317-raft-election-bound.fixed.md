- A broker bounds how many of its Raft groups are in an election at once (`RuntimeConfig::elections`, 64 by default),
  so thousands of groups that lose their leader together, or are formed together, settle instead of re-running
  elections whose vote saves outlast the timeout (#2317). New gauges `felix_broker_raft_elections_in_flight` and
  `felix_broker_raft_elections_waiting`.
