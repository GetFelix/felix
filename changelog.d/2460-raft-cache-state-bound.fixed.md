- A Raft cache shard's values and counters are bounded below the 1 GiB snapshot a member
  takes, so a member behind the purge or a new member can always catch up. A put or
  counter add past the bound is refused with `limit_exceeded` and `detail.reason` =
  `cache_full`; nothing is written, a retry gets the same answer, and deletes and expiries
  always apply. A leader no longer sends a snapshot over the cap: it counts
  `felix_broker_raft_snapshot_unsendable_total` and logs the group and member (#2460).
