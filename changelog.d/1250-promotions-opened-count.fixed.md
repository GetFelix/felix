- `felix_broker_promotions_opened_total` now counts a promotion when the
  shard is let go to serve rather than when its fence finishes, so a refused
  open that is retried is counted once (#1250).
