- The simulator's `torn-tail-repaired` check no longer fails when the simulator's own read of a stopped broker's disks opens a torn segment (#2195).
  Only a broker opening its log counts as the open that owes a repair.
