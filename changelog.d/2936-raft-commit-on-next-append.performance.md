- A Raft leader no longer sends each follower an extra append carrying only
  a new commit index after every write (#2936). That append now waits up to
  1 ms for the next one, which carries the commit instead, so one-at-a-time
  writes at RF 3 send two fewer peer messages each. With no write after it,
  a commit still reaches the followers within 1 ms.
