- The shard model now covers a staged destination that votes beside an RF 2
  failover (#1635). A leader restarted cleanly counts the staged copy toward a
  `Quorum` write, so the promoted leader's refusal of the crashed leader's
  fence is what keeps that write. The model checks that today's fence ids keep
  it and that leaving the staged copy out of them loses it.
