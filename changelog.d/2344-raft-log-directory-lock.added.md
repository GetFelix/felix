- A Raft log holds an exclusive `flock` on its directory while it is open (#2344).
  A second Raft open, from another process or the same one, fails as busy instead of writing beside the first.
