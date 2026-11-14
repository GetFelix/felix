- The seal that moves a shard to Raft goes over the wire (#2242): `SealFence`,
  `SealAck` and the `FromCopy` envelope, behind the new `SWITCH_SEAL` peer
  capability, which only a build with the Raft engine offers. A copy that
  promised a seal takes batches only from the sealing copy, refuses rebuilds,
  bootstraps and election ballots, and keeps its log from retention. A
  leader's batch that crossed its own promise is retried instead of fencing
  the follower, a seal acknowledgement on a large log is checked off the
  request and digested once, and a later generation's rebuild is not refused
  by an older promise. No
  existing frame changes, and nothing starts a seal yet.
