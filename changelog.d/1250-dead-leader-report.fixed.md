- A dead leader's last replica report no longer expires before placement can
  use it (#1250). Its age is now measured up to the leader's last heartbeat
  rather than up to now, so a slow placement pass or a control-plane outage
  around a leader's death no longer leaves the shard with no follower to
  promote. A leader that heartbeats but stops reporting still ages its report
  out. `felix_broker_promotions_opened_total` now counts a promotion only once
  the shard opens, so a refused open that is retried is counted once.
