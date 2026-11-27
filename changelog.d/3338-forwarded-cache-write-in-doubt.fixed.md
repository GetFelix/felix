- A cache write (put, conditional put, delete, counter add) that reached a Raft
  cache shard's leader through another broker, and whose outcome was unknown
  because the leader lost leadership or timed out before the commit, was answered
  as retryable `shard_unavailable`. A client that re-sent it could apply it twice:
  a counter add counted twice, a put restarted its TTL, and a conditional write
  was answered as refused for a write that had landed. It is now answered
  `leadership_lost` or `storage` (`outcome_unknown`). A forwarded write whose
  answer is lost or unreadable, a plain delete included, is reported
  `unacknowledged` instead of being sent again (#3338).
