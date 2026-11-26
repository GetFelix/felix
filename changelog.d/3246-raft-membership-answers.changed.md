- A Raft group's membership works out each member's role and voter flags once,
  when the membership is built, instead of on each of the several times openraft
  asks for every append (#3246). A lone write on a three-member group did about
  22 of these derivations on the leader and 39 on each follower; it now does none.
