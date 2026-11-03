- A subscriber that follows a shard move no longer skips a record that was waiting
  for the committed mark when the move came (#1835). A generation-start record
  written behind the held record moved the `shard_moved` resume position past it,
  though the record stayed on disk and the next leader committed it.
