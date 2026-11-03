- A subscriber of a leader that lost its shard could be told to resume past
  records it was never offered (#1837). When the old leader's log took the
  new leader's records before its readers were ended, the move's
  `resume_from` named the end of those records, and a batch it had held for
  the commit mark was skipped too. Readers are now ended as soon as such
  records arrive, at the position they had reached.
