- A shard leader sends a follower back from a cut what it missed within about a heartbeat, not after up to a second more (#2543).
  A Raft call whose deadline passes is now a timeout rather than an unreachable member, and the retry delays are capped at one heartbeat.
