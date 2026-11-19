- openraft moves to upstream commit `bc383014`, which times elections and heartbeats on
  deadlines instead of a ticker (openraft#2343, openraft#2378) and fixes futures held twice
  in the runtime (openraft#2400). Shard groups keep openraft's election timer off until
  the node ticker sees a member's leader is gone, so idle groups are not deposed between
  their slow heartbeats. The chrono-tz advisory ignores are gone with openraft's chrono
  dependency.
