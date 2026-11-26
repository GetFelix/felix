- On a Raft shard, plain publishes already queued on a shard's publish lane
  are proposed together as one batch, as the hybrid engine's claim does: one
  proposal, one openraft write, one commit wait and one delivery for up to 64
  publishes or 1 MiB. Each publish still gets its own offsets and its own
  answer. A lone publish waits for nothing more. If a leader change cuts a
  merged batch partway, every publish in it is answered as outcome unknown.
  A merged batch arms no answer timer only when every publish in it is a QUIC
  ack its control stream times out; otherwise it keeps one timer for all of
  them (#2936).
