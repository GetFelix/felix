- On Raft, an operator's move of a shard's leader to another member of its set completes while
  one member is down. It used to be accepted and then undone on the next placement pass. The
  group's leader now hands over to an operator's choice while a majority is caught up; the move
  is refused with `member_down` when fewer than a majority are serving, and given up after two
  minutes (`raft_leader_timed_out`) if it does not land (#3234).
