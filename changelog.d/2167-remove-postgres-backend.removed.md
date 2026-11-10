- Breaking: the control plane's Postgres metadata backend is gone (#2167). Raft is
  the one production backend and `memory` stays for tests and local runs. A control
  plane started with `FELIX_CONTROLPLANE_POSTGRES_URL` or
  `FELIX_CONTROLPLANE_STORAGE_BACKEND=postgres` now refuses to start, and a
  `postgres:` block in the config file is rejected, rather than falling back to an
  empty in-memory store. There is no migration: Felix is alpha, so stand up a Raft
  group (`FELIX_RAFT_NODE_ID`, `FELIX_RAFT_DATA_DIR`, `FELIX_RAFT_PEERS`) and recreate
  tenants, streams and caches. `felixctl controlplane migrate` (nothing writes a snapshot file any more),
  `--controlplane-bin`, `FELIX_CONTROLPLANE_BIN`, the
  `FELIX_CONTROLPLANE_POSTGRES_*` tuning variables, the 503 `busy` answer and
  `task pg:up`/`pg:down`/`pg:sweep` are removed too. The Helm chart's default
  backend is now Raft with three members, and `ci/postgres-values.yaml` is gone.
