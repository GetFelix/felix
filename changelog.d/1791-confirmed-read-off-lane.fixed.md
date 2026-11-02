- A copy with no promise no longer waits on the control plane while it
  answers a peer request (#1791). Its confirmed assignment read held the peer
  lane for a control-plane round trip, so during a bulk create every new
  shard's first fence stood in front of other shards' fences, batches and
  lease renewals. The broker now reads in the background, one read per shard
  at a time, refuses the fence or batch retryably until an answer arrives,
  and keeps the answer fresh while the leader keeps asking.
