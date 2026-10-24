- A replicated durable shard no longer writes its replica state on the
  acknowledgement path under `fsync_mode = on_commit` (#1456). The commit offset
  is written behind, and one write in flight carries every advance made while it
  runs, so a `Quorum` ack costs one data fsync per replica instead of about
  three. After a crash the commit offset can read back one write behind; the
  accepted generation is still on disk before a fence is answered.
