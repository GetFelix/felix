- Balance now moves an RF 1 Raft shard onto a live broker that leads two or
  more fewer than the shard's leader (#3118). Before, a broker with no RF 1
  shard, such as a new one or one a shard was moved off, never got one
  unless an operator moved it. The move is the one an operator move makes:
  the set grows to both brokers, then shrinks to the new one once its copy
  votes.
