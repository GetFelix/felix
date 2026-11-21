- A Raft shard under periodic fsync acknowledges a `Quorum` publish at a
  written majority again, without waiting for the leader to apply it
  (#2924). Followers on written-commit logs now send their written report
  after answering an append, as followers that fsync first already did. At
  RF 3 with one record per publish this cuts p99 from about 190 ms to about
  27 ms, level with the hybrid on the same machine.
