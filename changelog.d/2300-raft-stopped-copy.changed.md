- A Raft shard whose copy on a broker is stopped by a storage error, such as a
  failed fsync, no longer takes the whole broker out of `/ready`: only that
  copy stops, and the broker's other shards keep serving. The broker counts
  such copies in `felix_broker_raft_shards_stopped_on_storage_error`,
  `shard_inspect` shows the term and the error under `raft.stopped`, and a
  publish or subscribe for the shard on that broker is refused as `not_ready`
  with the error instead of "still opening the shard". A poisoned hybrid log
  and a failed node record still fail `/ready` (#2300).
