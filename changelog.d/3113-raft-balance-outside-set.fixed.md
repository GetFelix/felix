- A Raft broker that a drain swapped out of every replica set, such as one
  stopped with a shutdown hand-off and restarted, now gets shards back
  (#3113). Balance gives it a follower's seat and names it leader when it
  leads two or more fewer than a shard's leader and no member of the set
  does. Before, placement only renamed leaders within a set, so the broker
  stayed empty.
