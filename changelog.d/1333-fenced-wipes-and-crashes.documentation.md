- The TLA+ model now checks wipes, follower crashes and leader crashes on the
  fenced, follower-acked path the broker ships, not only on the report-gated
  one (#1333). Three new configurations pass (`FelixShardFencedWipe`,
  `FelixShardFencedCrash`, `FelixShardFencedLeaderCrash`), and each has a twin
  that drops one fix and finds the lost record. `FelixShardFencedCrash` is the
  first passing check of a follower crash with a rotated storage identity.
