- A planned leadership hand-over of a Raft shard (graceful shutdown, leader balance, an
  operator move) no longer answers a publish that was already in flight with
  `leadership_lost`. The old leader waits, within the same bound as before, until it has
  applied its whole log before it hands over, and a write of the closed term is answered
  by its commit (#3324).
