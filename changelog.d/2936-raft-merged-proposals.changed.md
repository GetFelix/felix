- On a Raft shard, plain publishes already queued on a shard's publish lane
  are proposed together as one batch, as the hybrid engine's claim does: one
  proposal, one openraft write, one commit wait and one delivery for up to 64
  publishes or 1 MiB. Each publish still gets its own offsets and its own
  answer. A lone publish waits for nothing more. If a leader change cuts a
  merged batch partway, every publish in it is answered as outcome unknown
  (#2936).
