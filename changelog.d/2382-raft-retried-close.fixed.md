- A retried `GroupRuntime::close` waits for a member that is still stopping and returns `true` (#2382).
  It used to answer "no group" at once, so a caller could close the log while the member could still write.
