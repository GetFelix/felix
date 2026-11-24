- A flush through io_uring (`FELIX_STORAGE_IO_URING=1`) whose caller stopped
  waiting now still poisons the log when its fsync fails (#2492). Before, the
  failure was only recorded by the caller, so a dropped flush could leave the
  log taking appends on top of a failed sync. A log also no longer has two
  fsyncs in the ring at once, so a later one that succeeds cannot acknowledge
  records an earlier failed one lost. A dropped `roll_now` likewise no longer
  leaves the roll slot taken.
