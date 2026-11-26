- A conditional head trim on a Raft shard no longer writes a trim below a
  floor the leader already answered. A majority can commit a trim before the
  leader's own log has synced it, and its floor applies on the leader only
  after that sync; a lower trim checked in between was written instead of
  answered as a no-op. The check now waits for pending trims to apply.
