- Evening out leader totals moves a stream shard instead of a cache shard
  (#1768). Placement used to move the first over-share shard in key order,
  and caches sort before most stream names, so a delete or a drain that left
  the totals two apart moved a cache and fenced it for every client using it.
  A cache now moves for the totals only when no stream shard can.
