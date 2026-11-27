- A broker learns a Raft shard's new membership target as soon as the control plane
  writes it after a move, drain, top-up or replace, instead of up to one sync interval
  later. A change on the assignment feed that moved nothing on the broker, such as
  another broker's copy id, left the watch idle for the rest of the interval (#3339).
