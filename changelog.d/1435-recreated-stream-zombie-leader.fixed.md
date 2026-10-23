- A stream deleted and created again under the same name without incarnations no longer leaves a
  zombie leader behind (#1435). The delete now puts an `unassigned` change for each shard on the
  shard change log, so a broker leading one lets it go. The recreated stream's shards start at the
  change seq instead of generation 0, because brokers keep an unincarnated stream's logs and the
  generations those logs accepted: a broker that had taken a shard by a move refused to lead or
  follow the new shard at generation 0. Under Raft both need every member at metadata level 17. A
  broker also takes a shard's first assignment over whatever it still holds, which covers control
  planes that don't record the unassignments yet.
