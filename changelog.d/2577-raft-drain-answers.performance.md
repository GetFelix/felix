- A Raft shard's publish task is woken once per call to openraft instead of
  again for every batch's answer (#2577). Each record now carries a
  responder that tells its batch directly where openraft applies or refuses
  it, so a drain of many batches costs one task wake and one channel send
  per batch, not one per record plus a hop through the publish task.
