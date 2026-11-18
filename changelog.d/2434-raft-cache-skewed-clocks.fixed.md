- The members of a Raft cache group hold the same cache values and counter
  sums however far apart their clocks run, including across a leader change,
  and a test now holds the state machine to that (#2434). Only the leader
  reads its clock, to stamp entries and to judge reads and expiry.
