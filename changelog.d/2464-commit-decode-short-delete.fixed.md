- A commit record whose operations are deletes of short keys now decodes. The
  decoder assumed every operation took at least nine bytes, so it refused such a
  record and the shard's state could not be rebuilt from its log.
