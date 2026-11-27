- A cache write (put, conditional put, delete, counter add) that reached a Raft
  cache shard's leader through another broker, and whose outcome was unknown
  because the leader lost leadership or timed out before the commit, was answered
  as retryable `shard_unavailable`. A client that re-sent it could apply it twice:
  a counter add counted twice, a put restarted its TTL, and a conditional write
  was answered as refused for a write that had landed. It is now answered
  `unacknowledged` when the leader lost leadership and `storage` when the commit
  wait ran out, both `outcome_unknown`. A forwarded write whose answer is lost or
  unreadable, a plain delete included, or that was in flight when the forwarding
  broker shut down, is reported `unacknowledged` instead of being sent again. A
  cache write a leader refused because its lease had lapsed is answered
  retryable `shard_unavailable`, directly or forwarded, since nothing was
  written. A forwarded cache write too large for the shard's log is answered
  `invalid_request`, as the same write sent directly is (#3338).
