- A metadata Raft leader that loses its term while judging a heartbeat, an
  expiry sweep or a placement-lease request no longer acts on that judgement
  (#2881). Before, a deposed leader forwarded its `ExpireNodes` or
  `TakePlacementLease` to the new leader, which committed it without having
  seen what it was judged from, so it could expire a live node or give the
  placement lease a second holder. Such a request now answers "not leader" and
  the caller asks again.
  A member still on an older build can forward these until it is upgraded.
