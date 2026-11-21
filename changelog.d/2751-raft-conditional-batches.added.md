- A Raft shard takes a conditional publish of several records (#2751). It is
  written whole at the expected offset or refused whole, readers never see
  part of it, and a refused batch consumes one offset per record. A cluster
  takes such a batch once its fleet finalizes `raft_conditional_batches`,
  which a new cluster does at once; a lone broker takes it from the start.
