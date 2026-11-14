- openraft moves to upstream commit `5c3ad1bc2d` (#2415). A Raft member starts its client responder
  queue at 64 slots instead of 8,192, which drops about 320 KB of reserved memory per member.
