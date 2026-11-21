- `GroupRuntime::handle`, which skipped the Raft sender checks, is now
  `GroupRuntime::handle_unchecked` behind the `testing` feature. Peer messages
  reach a member only through the internal listener, which checks every one.
