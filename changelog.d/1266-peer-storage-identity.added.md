- Brokers exchange storage identities in the peer handshake, behind a new
  capability bit, `STORAGE_IDENTITY` (`1 << 10`), so an older peer sees the
  frames it always did (#1266). A leader asks for each follower's identity at
  the start of every exchange and starts the follower's cursors over when it
  changes, so a follower that lost its disk is no longer reported caught up
  on an idle shard. Replica reports carry the identity each follower was
  counted under as `replica_storage_ids`, omitted when empty. Placement does
  not read it yet.
