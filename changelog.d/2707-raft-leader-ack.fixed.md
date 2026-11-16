- A `Leader` publish on a Raft shard whose leader lease does not hold is answered where a `Quorum` publish would be,
  not at its fsynced commit. A single-node Raft shard never holds the lease, so its `Leader` publishers waited on an
  fsync that `Quorum` publishers on a `periodic` broker skip (#2707).
