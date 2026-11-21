- A Raft control plane no longer hands the placement lease to a second
  holder while the first one's lease runs (#2886). When the metadata leader
  declined soft state, because a member had not reported its level since the
  leader started, the asking instance proposed the take itself and the log
  applied it without checking expiry. The leader now judges the lease at
  every level, and no instance proposes the take on its own.
