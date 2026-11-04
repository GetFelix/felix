- A commit record whose operations were all deletes of short keys no longer fails
  to decode (#1051). The size check assumed nine bytes per operation, but a delete
  takes five, so rebuilding a shard's state from its log refused such a commit.
