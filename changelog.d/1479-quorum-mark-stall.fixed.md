- A `Quorum` stream on three or more brokers no longer stalls for up to the
  replication tick (2 s by default) under load (#1479). A follower's answer that
  arrived after the pass that asked for it moved no cursor, so no pass counted
  it, and with every publisher waiting on the mark nothing else started one.
  Such an answer now starts a pass, and a pass waiting for a majority counts a
  busy follower's answer itself instead of only giving up on it. The stall was
  most visible once `majority_ack` is finalized: 256 B durable publishes at 64
  in flight fell 38% with a p999 of 1.86 s.
