- Raft: a consumer group that settled past its own trailing entries no longer describes a
  committed offset above the shard's tail, and a read-index round no majority answers is
  refused after the lease instead of the election timeout, so a group call to a cut-off
  leader fails fast and retryably.
- Raft: `shard_inspect` on a Raft shard's leader reports the log's tail again; the log is
  opened at the shard's incarnation, which the inspector did not look up.
