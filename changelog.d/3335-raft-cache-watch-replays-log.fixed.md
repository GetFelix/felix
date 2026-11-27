- A cache watch on a Raft shard that resumes from an older offset, a watch from offset 0
  on a shard nobody watched included, is replayed from the member's Raft log, as the
  hybrid replays its shard log, instead of being handed a resnapshot. Only a resume from
  an offset the member's snapshot already covers is resnapshotted (#3335).
