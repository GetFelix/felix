- A publish to a Raft shard looks its shard's group up once instead of at
  every step (#3233). Each lookup is a read lock and a hash of the shard
  key. From the QUIC publish lane to its commit a publish did eight and now
  does two: the publish itself and the broker's read bound before it
  releases the batch. The control stream's check that the shard is on Raft
  is one more, as before. A publish also no longer copies the shard key for
  its answer, copies its publisher into a batch of one, or allocates a
  shared tally to answer a batch of one record. A publish that found a
  group which then closed or was replaced is refused as one that found no
  group.
