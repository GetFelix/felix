- Readers of a Raft shard never see part of a multi-record conditional batch
  (#2751). The read bound stays at a batch's first record until the batch is
  whole, a batch a leader change cut is skipped like blank entries and counted
  as skipped in the next record, and a blanked batch releases all its offsets.
  Multi-record conditional batches stay behind their fleet gate.
