- A Raft log holds an exclusive `flock` on its directory while it is open (#2344).
  A second open, from another process, a restore tool or the same process, fails as busy instead of writing beside the first.
