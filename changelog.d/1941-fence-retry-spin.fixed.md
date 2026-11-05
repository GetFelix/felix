- A promoted leader whose fence failed no longer spins its replication loop
  once the shard's route names another leader, or once it stands for a
  generation no route names yet (#1941). The fence retry kept coming due with
  a zero wait, using a core until the route changed. It now waits for the
  route, as the rest of replication does.
