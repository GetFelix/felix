- After a control-plane restore of a backup older than a broker's last registration, the
  heartbeat answer now carries `register_again: true` and the broker registers again on its
  own, keeping its lease, so its placement acknowledgements and Raft copy ids count without
  a restart (#2604). A broker offers this by sending `registered_at_millis` in its
  heartbeat; an older broker gets the same answer as before.
