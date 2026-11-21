- Raft members held after a restart no longer all stand the moment the hold
  ends (#2895). Each waits past the hold by its own jitter, so a group whose
  leader did not come back elects one in a single round instead of splitting
  its votes.
