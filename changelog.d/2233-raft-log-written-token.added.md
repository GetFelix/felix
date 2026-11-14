- The Raft log adapter tells a proposer where its entry was written (#2233). A command may
  carry a `WrittenToken`, in memory only, and `append` resolves it with the entry's index and
  term once the entry is written and before the fsync. Nothing sets one yet.
