- A Raft copy with no `raft-epoch` file in a group past epoch 0 (a learner
  placed fresh, or a directory restored from an older backup) no longer
  reports its offset floor at epoch 0 or its copy id (#2690). The floor
  report was refused and retried forever, stalling the group's floor, and the
  copy id would have let the copy become a voter at an epoch it cannot prove.
