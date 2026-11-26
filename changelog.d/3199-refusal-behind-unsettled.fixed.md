- A publish to a Raft shard that the leader refuses before writing any of it
  (its lease lapsed, or it no longer leads the term) is now answered at once
  with that refusal, `shard_unavailable` / `not_ready`, which a client may send
  again (#3199). Before, it waited behind an earlier batch that had been
  acknowledged and not yet committed, and if that batch never settled the
  publisher got `quorum_timeout` 10 s later, an outcome-unknown answer for a
  batch that was never written. The `quorum_timeout` message no longer says the
  batch is durable on the leader: it reads "the batch did not reach a majority
  within ...", since a Raft leader can time out before its log has the batch.
