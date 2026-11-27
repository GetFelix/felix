- On the Raft shard engine, draining a broker that is the only member of a shard whose
  replication factor is above 1 now finishes (#3303). The set grows by one live broker
  while the draining broker keeps leading, the lead moves once the new copy votes, and the
  old copy is then swapped out.
