- On a Raft shard, a broker whose member follows a known leader now serves
  subscribes from its own log, up to what it has committed, instead of
  redirecting them to the leader (#2454). Offsets match the leader's. A member
  with no known leader refuses subscribers, and a resume offset past a
  follower's log is redirected to the leader. `felix_broker_raft_shards_led`
  counts the Raft shards a broker leads.
