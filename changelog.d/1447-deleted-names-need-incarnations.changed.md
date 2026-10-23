- The control plane refuses with 409 to create a stream or cache under a name that was deleted on
  the cluster until `stream_incarnations` (or `cache_incarnations`) is finalized, and the message
  says to finalize first (#1447). Without an incarnation, a stream created again opened the
  deleted stream's logs on every broker, so a recreated `Quorum` stream's followers held old
  committed records that the new leader's writes disagreed with, and they halted as diverged.
  Every store records deleted names (Postgres migration 0035; on Raft once every member is at
  metadata level 18), and they are part of the exported state.
