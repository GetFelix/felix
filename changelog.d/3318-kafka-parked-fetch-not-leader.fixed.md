- A Kafka fetch long-polling on a broker that stops leading its partition is answered
  `NOT_LEADER_OR_FOLLOWER` when it next wakes, instead of returning records once more
  from the broker's follower copy on Raft and hearing "not leader" one wait later (#3318).
