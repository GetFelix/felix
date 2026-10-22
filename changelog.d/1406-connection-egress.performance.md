- The client writes all of a connection's publish streams from one task, so frames queued on
  different shard streams go out in the same packets. Per-shard streams each held one or two
  frames, which put tiny datagrams on the wire and cost the broker a receive per frame (#1406).
  A stream that cannot write because of flow control is skipped; it never holds up the others.
