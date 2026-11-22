A Raft publish answered at its commit is woken once, by openraft's answer at the apply, instead of first by its write and then by the commit.
