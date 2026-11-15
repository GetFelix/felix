- A `Quorum` write waiting for a written majority on a Raft shard is woken only by a report that can complete it, not by every follower report (#2581).
  300 waiting writes at five voters went from 16,470 looks to 300.
