- A new Raft shard leader no longer waits up to a full assignment-feed long
  poll (about 20 s) for its target (#2679). A long poll held from before the
  term could not tell the target, so a membership change in flight sat until
  it timed out. The new term now asks the watch for a fresh view at once.
