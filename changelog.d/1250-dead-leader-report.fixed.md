- A dead leader's last replica report no longer expires before placement can
  use it, once the fleet has finalized `generation_start` and `majority_ack`
  (and `fenced_caches` for caches) (#1250). Its age is then measured up to the
  leader's last heartbeat, so a slow placement pass or a control-plane outage
  around a leader's death no longer leaves the shard with no follower to
  promote. A leader that heartbeats but stops reporting still ages its report
  out, and a fleet where a promotion can open on the lease keeps the old
  expiry. `felix_broker_promotions_opened_total` now counts a promotion when
  the shard is let go rather than when its fence finishes, so a refused open
  that is retried is counted once.
