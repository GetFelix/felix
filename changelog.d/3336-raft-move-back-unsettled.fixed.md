- Moving a one-replica Raft shard back to its old broker right after the move started
  answers 409 `set_unsettled`, and succeeds on retry once the grown set's copy ids and
  voters are reported, instead of a 500 (#3336). A write the store refuses from the move
  API is now 409 `conflict`, never a 500.
