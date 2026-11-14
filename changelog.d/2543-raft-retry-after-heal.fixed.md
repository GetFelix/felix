- A shard leader sends a follower back from a cut what it missed within about a heartbeat, not after up to a second more (#2543).
  The retry delays are capped at one heartbeat, and a heartbeat or vote whose deadline passes is a timeout rather than an unreachable member.
