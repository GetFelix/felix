- An elected leader whose replica set is older than the control plane's is now
  named on the control plane's set, with nothing pending (#1970). The control
  plane used to record the older set and leave its own as a pending change,
  which dropped the set another candidate may already have opened and
  acknowledged on. Under Raft, adopting such a leader waits for metadata level
  29 on every member; moving past a generation claimed twice does not need it.
