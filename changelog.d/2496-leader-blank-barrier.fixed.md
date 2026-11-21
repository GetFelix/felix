- A control-plane member that becomes the metadata Raft leader no longer
  answers heartbeats, expiry sweeps or placement-lease requests until it has
  applied its term's blank entry (#2496). Before, a leader restarted with
  committed entries it had not applied yet could hand the placement lease to a
  second holder, or expire a node, on that older state.
