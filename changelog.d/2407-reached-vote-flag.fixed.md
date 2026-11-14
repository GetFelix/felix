- A metadata Raft member whose log directory
  was emptied no longer forms the group again (#2407). The forming mark
  lives beside the log, not in it, so the member kept its mark while its
  vote was gone, went on forming and could vote twice in one term. A member
  now records that it reached the vote before it first votes, and one that
  holds that record with a blank log catches up with a leader instead.
