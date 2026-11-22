- A newly elected metadata Raft leader no longer judges a join, whether to
  feed a member, or the level it settles a proposal at, by a join floor older
  than its committed log (#2878). Before, a leader that had not yet applied the
  entry raising the floor could add a member below it, send it entries it
  cannot apply, or write a command at a level below what the log needs.
