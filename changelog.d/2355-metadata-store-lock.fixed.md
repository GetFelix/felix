- The metadata store's lock holds up in three places it did not (#2355).
  Two first starts of one data directory no longer share `store.tmp`: the
  build runs under an exclusive lock on the data directory, and the second
  start finds the store built. The store's lock now lives as long as its log,
  so a clone of `store.log()` that outlives the store keeps the store locked
  while its flusher can still write, and so does a clone of
  `store.snapshots()`. A shutdown closes both, so a clone left over cannot
  append, vote, give up a generation, purge or install a snapshot after the
  lock is released. The crash tests wait for the committed
  id write with a bound and fail if it does not finish. `flock` on Linux NFS
  is emulated per process; the data directory belongs on a local disk.
