- A promoted leader whose fence failed no longer spins its replication loop
  (#1941). Once the fence's backoff passed, the retry kept coming due with a
  zero wait whenever the broker could not act on it: until the next routing
  wake when the shard had moved to another leader, and until the route caught
  up when the broker stood for a generation no route names yet. It now waits
  for the route, as the rest of replication does.
