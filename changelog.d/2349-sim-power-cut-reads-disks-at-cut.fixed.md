- felix-sim reads the disks at the instant of a one-broker power cut, and no longer counts a record acknowledged during the cut as a copy the cut took from a disk that never held it (#2349).
  A later loss of that record's one remaining copy is now a finding instead of excused.
