- A broker promoted before its registration answered no longer opens a shard
  on the lease (#1261). Until then its fleet view is empty, so on a fleet that
  finalized `generation_start` and `majority_ack`, an unreachable old leader
  read as a peer without the fence and the shard opened without fencing a
  majority, which could lose records the old leader acknowledged. The shard
  now stays closed until the broker has its fleet view, then fences as usual.
