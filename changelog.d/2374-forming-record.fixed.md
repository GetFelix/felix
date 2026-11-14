- A metadata Raft member that loses its volume in a cluster's first moments
  no longer forms the group a second time (#2374). Before the first leader
  writes an entry every log is empty, so under
  `FELIX_RAFT_INITIAL_CLUSTER_STATE=new` a wiped member saw only empty
  answers, formed again and could vote twice in one term. Forming is now
  mark, record, vote: the member writes a mark to its own store, every
  other member records its id over the new peer route
  `POST /internal/raft/v2/forming`, and only then does it vote. A member that a
  peer has recorded and that holds no mark waits for a leader and catches up
  instead. A cluster's first start therefore needs every control-plane
  member on a release with the route; a group that already runs is not
  affected.
