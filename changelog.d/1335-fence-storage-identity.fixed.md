- A promoted leader's fence no longer counts a replica that came back under a
  new storage identity (#1335). Under `Periodic` or `None` fsync, a follower that
  crashed lost records it had already answered for. If the leader was then
  replaced, the fence could count that short copy toward its majority, and a
  record acknowledged on the old leader and that follower was lost. A failover's
  assignment now carries the storage identity the replica report named for each
  replica (`fence_storage_ids`). The fence counts an answer only when it comes
  over a connection whose handshake gave that identity. Any other answer still
  takes the promise, but counts for nothing, and the fence waits for a copy that
  can vouch for what it holds. Under Raft the field waits for metadata level 16.
  A member of the old set the report could not vouch for is named with an empty
  identity and does not count. A replica outside the old set, or one from a
  build that predates identities, counts as before. With the old leader
  gone for good and no majority that counts, the shard waits. The way out is
  to stop the promoted broker and run `felixctl placement abandon`. That keeps
  every copy in the new replica set and takes the furthest of them.
  Under `Periodic`, the default, this includes a leader and a follower killed
  together. The follower comes back under a new identity even when its page
  cache kept everything, so the shard no longer serves when it returns. It
  waits for the operator instead.
