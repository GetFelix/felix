- Shard directories a Raft copy rotation moved aside are now deleted (#2442).
  They move into `.kept/` and go once the copy now in the shard directory has committed past their last record, or when their stream is deleted.
  `felix_storage_raft_kept_copies` and `felix_storage_raft_kept_copy_bytes` report what is left.
