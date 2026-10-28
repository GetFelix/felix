- A broker whose data directory was wiped no longer takes a deposed leader on
  the word of a lagging control-plane member (#1332). Its routing view is
  seeded from whichever member answers, which under Raft can still name a
  leader already replaced, and its vote could then complete that leader's
  majority. A copy with no election promise on disk now also checks the
  shard's assignment as the control plane's Raft leader confirms it, through
  the new `GET /v1/shard-assignments/confirmed`, before it takes a leader. A
  control plane without the route leaves the routing view to answer as before.
