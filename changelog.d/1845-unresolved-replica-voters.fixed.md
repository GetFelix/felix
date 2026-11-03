- A replica the node catalog does not name no longer shrinks its shard's
  majority (#1845). A broker builds routes before its first `/v1/nodes` read
  succeeds, and such a replica used to be dropped from the route. With an empty
  catalog a promoted leader of three opened without fencing either follower,
  and a `Quorum` write was acknowledged on the leader alone. The replica now
  stays in the route without an address and counts as one that has not
  answered, so the shard waits for a real majority.
