- A planned leadership hand-over of a Raft shard (graceful shutdown, leader balance, an
  operator move) now answers a publish already in flight by its commit when it commits
  before the hand-over, instead of with `leadership_lost`. The old leader waits, within
  the same bound as before, until it has applied its whole log before it hands over. A
  publish can still, rarely, get `leadership_lost` (outcome unknown) when it is appended
  just after that check or the bound runs out; it is never acked unless it committed
  (#3324).
