- Raft logs that save a vote in the same moment now share flushes of their device (#2317). Each
  vote took two to four fsyncs of its own before, so many groups electing at once could take
  longer than the election timeout and never settle. A vote is still on disk before it is answered.
