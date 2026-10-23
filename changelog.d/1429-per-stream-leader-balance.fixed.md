- Placement now spreads each stream's and cache's leaders evenly across brokers, not only the
  cluster-wide total (#1429). A 12-shard stream on three brokers could be placed 0/0/12 beside
  other streams that already filled two brokers to their share, and rebalancing left it at 1/1/10
  because the totals were even, so the stream's writes ran on one broker. New shards now go to
  the broker leading fewest of their stream first, a broker leading two or more of a stream's
  shards more than another hands one over, and a move for the cluster-wide balance never makes a
  stream's spread wider. The totals are now evened to within one, where before a broker was only
  moved off when it led more than its rounded-up share. Existing clusters even out after the upgrade, one move at a time under
  the usual move limits.
