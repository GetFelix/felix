- The hybrid side of a shard's switch to Raft: a copy promises the seal on
  disk, the seal leader appends the record at the base naming its start and
  the digests of the shard's log and its dead-letter and group-cursor logs,
  and a follower acknowledges only once its own copy matches and the record
  is on disk. A voter's start checks the side logs and the record's payload
  too (#2242). Nothing seals a shard yet.
