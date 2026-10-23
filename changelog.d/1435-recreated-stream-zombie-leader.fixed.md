- A broker that took a shard by a move no longer keeps leading it after its stream is deleted and
  created again under the same name without incarnations (#1435). The new stream's first
  assignment starts at generation 0, below the moved shard's, and the broker ignored it as stale:
  it kept replicating and reporting the deleted shard, and a move of the new shard to it never
  finished. A shard's first assignment now replaces whatever the broker held for it, and a broker
  leading the new shard reopens it.
