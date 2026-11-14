- A shard leader sends a follower back from a cut what it missed within about a heartbeat, not after up to a second more (#2543).
  When a peer broker answers again after a failed call, every leader on the broker retries its members there at once instead of waiting out openraft's retry delay.
