- A write waiting on a Raft shard is woken only by the apply or flush that reaches it, not by every one (#2577).
  With 512 writes in flight, an advance no longer wakes all 512 to look and sleep again.
