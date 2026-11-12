- The simulator's `torn-tail-repaired` check no longer fails on a clean log (#2195).
  The simulator's own read of a stopped broker's disks no longer counts as the open that owes a repair, and a power cut that keeps only whole records of a multi-record append is no longer held to one.
