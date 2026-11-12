# controlplane service

Executable entrypoint for the Felix control plane.

Responsibilities:
- Serves the REST API for tenants, streams, caches, membership and shard assignments
- Coordinates metadata and placement
- Runs as one instance or several, over an embedded Raft group

Local dev:
- `cargo run -p felix-controlplane-service`
- OpenAPI spec: `http://localhost:8443/v1/openapi.json`

## Storage backends

- **Raft (production):** The instances form a Raft group and hold the metadata themselves, with no external database. Select it with `FELIX_CONTROLPLANE_STORAGE_BACKEND=raft`, or by setting `FELIX_RAFT_NODE_ID`, `FELIX_RAFT_DATA_DIR` and `FELIX_RAFT_PEERS`; see [docs/metadata-raft-design.md](../../docs/metadata-raft-design.md).
- **Memory (default when no Raft variables are set):** No external dependencies; state lives in one process and is lost on restart. For local development and tests.

There was a Postgres backend. It was removed; the control plane refuses to start when `FELIX_CONTROLPLANE_POSTGRES_URL` or `FELIX_CONTROLPLANE_STORAGE_BACKEND=postgres` is still set.

Example YAML override (`FELIX_CONTROLPLANE_CONFIG`):
```yaml
bind_addr: "0.0.0.0:8443"
storage:
  backend: raft
changes_limit: 1000
change_retention_max_rows: 10000
oidc_allowed_algorithms: ["ES256", "RS256"]
```

Environment variables (take precedence over YAML):
- `FELIX_CONTROLPLANE_STORAGE_BACKEND` (`memory`|`raft`)
- `FELIX_CONTROLPLANE_CHANGES_LIMIT`
- `FELIX_CONTROLPLANE_CHANGE_RETENTION_MAX_ROWS`
- `FELIX_CONTROLPLANE_OIDC_ALLOWED_ALGORITHMS` (comma-separated; default: `ES256,RS256`)
