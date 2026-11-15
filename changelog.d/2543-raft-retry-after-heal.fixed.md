- A shard leader sends a follower back from a cut what it missed within about a heartbeat, not after up to a second more (#2543).
  When a peer broker answers again after its calls kept failing, every leader on the broker retries its members there at once instead of waiting out openraft's retry delay. A group that opens late on a restarted broker is retried on its own member's first answer.
