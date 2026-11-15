- A Raft shard's publish task hands openraft each drain of term-bound batches as one
  `client_write_many` call with `WriteOption::expect_leader`, instead of one conditional write per
  record. openraft admits or refuses the call whole, so a lapsed write lease can no longer leave a
  batch with a hole in it, and no other write lands between a batch's records.
