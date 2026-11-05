- An elected leader whose fence covered the control plane's replica set
  beside its own is now named on the control plane's set, with nothing
  pending (#1970). The control plane used to record the elected set and leave
  its own as a pending change, which dropped the set another candidate may
  already have opened and acknowledged on. Brokers now report that set in
  `elected.leaving`. Under Raft, adopting such a leader waits for metadata
  level 28 on every member; moving past a generation claimed twice does not
  need it.
