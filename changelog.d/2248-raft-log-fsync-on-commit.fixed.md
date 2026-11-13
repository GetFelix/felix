- A Raft log now refuses to open under any fsync mode but `FsyncMode::OnCommit` (#2248). Under the other
  modes a member would acknowledge an entry before it was on disk.
- A Raft log now saves the vote of the leader a majority elected when it had voted for another
  candidate in that term (#2231). It refused that vote before, which stopped the member.
