- Evening out leader totals moves a stream shard instead of a cache shard
  (#1768). Placement used to move the first over-share shard by name, often
  a cache, so a delete or a drain that left the totals two apart moved a
  cache and fenced it for every client using it. Caches are now planned
  after the streams, so a stream shard goes first whenever one can.
