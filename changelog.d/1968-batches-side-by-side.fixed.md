- A follower now stores the record batches on one peer stream side by side, up
  to 16 at once per stream, 64 per peer node and 256 across the broker, and
  answers each as it finishes (#1968). Stored in turn, a batch waited behind every batch ahead of
  it on its stream, so under load a leader's round trip to a follower grew far
  past the follower's store time. Bootstraps and rebuilds are still answered
  in turn. Per-shard order is unchanged: a leader has one batch in flight per
  follower and log.
