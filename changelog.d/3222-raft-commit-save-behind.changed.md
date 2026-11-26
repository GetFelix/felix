- A Raft log saves its committed index behind the commit under every fsync mode, not
  only `on_commit`, so a group no longer waits on a replica-state write (a create, two
  fsyncs and a rename) before it applies, once a second per committing log (#3222).
