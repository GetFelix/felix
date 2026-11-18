- A test now checks that the members of a Raft cache group hold the same
  state after every write when their clocks are a second apart and leadership
  moves between them (#2434). The state machine's no-clock check also covers
  the overlay maps and the apply loop, and fails if they reach the default
  engine's cache or counter logs, which stamp records from the local clock.
