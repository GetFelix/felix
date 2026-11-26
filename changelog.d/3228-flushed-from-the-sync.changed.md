- Faster: on a broker that commits on a written majority (`periodic` or `none` fsync), a Raft
  log's flushed position is raised by the fsync that covers an entry instead of by a flush task
  each append woke, so an append no longer wakes another task (#3228). What openraft is told,
  and when, is unchanged.
