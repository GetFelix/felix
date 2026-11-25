- A Raft leader now asks its followers for written reports as soon as a
  `Quorum` write waits on them (#3117). Before, a write that committed
  before its wait first ran left the leader not asking, so the next
  `Quorum` write on a stream that does not fsync on commit was acknowledged
  only once the leader had applied it, not at the written majority.
