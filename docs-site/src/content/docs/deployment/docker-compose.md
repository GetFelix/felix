---
title: "Docker Compose Deployment"
description: "Run the control plane and a broker under Docker Compose, with the credential, storage and metrics a broker needs."
---

Running Felix under Docker Compose, for local development and testing.
With Podman, run the same files with `podman compose`; see
[Docker or Podman](/getting-started/containers/) for the provider it needs and the few places the two
differ.

The images are published to GHCR and are pullable without credentials:

```bash
docker pull ghcr.io/getfelix/felix-broker:0.6.0-preview.3
docker pull ghcr.io/getfelix/felix-controlplane:0.6.0-preview.3
```

Releases before 0.6.0-preview.2 are under `ghcr.io/gabloe`, the project's previous owner.

Each release publishes the full version (`0.6.0-preview.3`). A release without a
pre-release suffix also publishes its minor series (`0.6`) and `latest`. Use a
full version tag in anything you deploy, because the other two move. A client
negotiates its features with the broker it connects to, so a newer client
against older images runs without whatever the images predate.

To build them yourself instead, for a change you have not released or an
architecture the release does not build:

```bash
docker build -f docker/broker.Dockerfile -t felix-broker:dev .
docker build -f docker/controlplane.Dockerfile -t felix-controlplane:dev .
```

Both build from the repository root. The binaries are workspace members, so
cargo needs the whole workspace to resolve them. Each Dockerfile takes one
build argument, `BIN`, naming the binary to build. Docker warns about any
other `--build-arg` and ignores it. `podman build` needs `--format docker`, or
it drops the images' `HEALTHCHECK`.

:::note[Compose vs Kubernetes]
Use Docker Compose for local development and testing. For production, see the [Kubernetes guide](/deployment/kubernetes/).
:::

## What a broker needs

A broker cannot run on its own. Before writing a compose file, know what it
expects:

- **`FELIX_CONTROLPLANE_URL`.** Required. The broker fetches the keys that
  verify client tokens from the control plane, and it exits at startup
  without the URL:

  ```
  Error: FELIX_CONTROLPLANE_URL must be set for auth
  ```

- **A node credential**, as `FELIX_NODE_TOKEN_FILE` (or `FELIX_NODE_TOKEN`).
  The control plane's metadata feeds (tenants, namespaces, streams, caches)
  answer only a Felix token carrying `node.view:cluster:*`. Without one the
  broker starts but never learns that any stream exists. See
  [Broker credential](#broker-credential).
- **A storage directory**, if you want durable streams. With
  `FELIX_DURABLE_STORAGE_DIR` unset the broker keeps nothing on disk. The image
  declares `/var/lib/felix` as a volume for this. Point the variable at it.

The images run as uid and gid `65532`. A named volume inherits the right
ownership from the image. A bind mount must be writable by that uid.

## Broker and control plane

A control plane over Postgres and one durable broker:

```yaml
services:
  postgres:
    image: docker.io/library/postgres:16-alpine
    environment:
      POSTGRES_USER: felix
      POSTGRES_PASSWORD: felix
      POSTGRES_DB: felix
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U felix -d felix"]
      interval: 5s
      timeout: 3s
      retries: 5

  felix-controlplane:
    image: ghcr.io/getfelix/felix-controlplane:0.6.0-preview.3
    environment:
      - FELIX_CONTROLPLANE_POSTGRES_URL=postgres://felix:felix@postgres:5432/felix
      # Day 0 only: the bootstrap API is how the broker credential is made.
      # Remove these three once it exists.
      - FELIX_BOOTSTRAP_ENABLED=true
      - FELIX_BOOTSTRAP_BIND_ADDR=0.0.0.0:9095
      - FELIX_BOOTSTRAP_TOKEN=change-me
      - RUST_LOG=info
    ports:
      - "8443:8443"            # REST API
      - "127.0.0.1:9095:9095"  # bootstrap, loopback only
    depends_on:
      postgres:
        condition: service_healthy

  felix-broker:
    image: ghcr.io/getfelix/felix-broker:0.6.0-preview.3
    environment:
      - FELIX_CONTROLPLANE_URL=http://felix-controlplane:8443
      - FELIX_NODE_TOKEN_FILE=/run/secrets/felix-node-token
      - FELIX_DURABLE_STORAGE_DIR=/var/lib/felix
      - RUST_LOG=info
    ports:
      - "5000-5003:5000-5003/udp"  # client QUIC, one port per listener
      - "8080:8080"      # metrics, /live, /ready
    volumes:
      - felix-data:/var/lib/felix
    secrets:
      - felix-node-token
    depends_on:
      felix-controlplane:
        condition: service_healthy
    restart: unless-stopped

secrets:
  felix-node-token:
    file: ./felix-node-token

volumes:
  felix-data:
```

To run your own build, replace an `image:` line with
`build: { context: ., dockerfile: docker/broker.Dockerfile }` (or the
control-plane Dockerfile).

Both images carry a `HEALTHCHECK` against `/ready` on port 8080, so
`service_healthy` works without declaring one. A durable broker's `/ready`
answers 503 until it has synced its catalog from the control plane, so a
broker whose credential is missing or wrong stays unhealthy.

Start it and watch the broker come up:

```bash
docker compose up -d
docker compose logs -f felix-broker
curl http://localhost:8080/ready
```

### Broker credential

The credential is a Felix token carrying `node.view:cluster:*`. Cluster scope
cannot be granted by a tenant admin, so it comes out of bootstrap: initialize a
tenant with a broker role, assign the broker's principal to it, then exchange
an IdP token for that principal.

```bash
curl -sS -X POST http://127.0.0.1:9095/internal/bootstrap/tenants/ops/initialize \
  -H 'X-Felix-Bootstrap-Token: change-me' \
  -H 'Content-Type: application/json' -d '{
    "display_name": "Operations",
    "idp_issuers": [ ... ],
    "initial_admin_principals": ["p:admin"],
    "policies": [
      { "subject": "role:broker", "object": "cluster:*", "action": "node.view" }
    ],
    "groupings": [
      { "user": "p:broker", "role": "role:broker" }
    ]
  }'
```

The token exchange needs an identity provider that the tenant trusts
(`idp_issuers`). For a development stack with no identity provider, set
`FELIX_BOOTSTRAP_DEV_TOKENS=true` and ask the bootstrap listener for a
[development token](/features/security/#development-tokens) for
`p:broker` with `"audience": "felix-controlplane"` instead. Otherwise exchange with
`"audience": "felix-controlplane"` and write the Felix token to
`./felix-node-token`. The
[bootstrap flow](/features/security/#bootstrap-mode-day-0) and
[token exchange](/features/security/#token-exchange-oidc--felix) cover
the request bodies.

The token expires like any Felix token, after 15 minutes for an exchanged one.
Every broker, this one included, re-reads `FELIX_NODE_TOKEN_FILE` every 30
seconds, so whatever mints the credential can rewrite the file. Or give the
broker a refresh token at `FELIX_NODE_REFRESH_TOKEN_FILE` (a writable path) and
it renews its own token before it expires; see
[Kubernetes](/deployment/kubernetes/).

To try Felix without any of this, `task cluster:up` starts a control plane and
brokers on your machine and mints the credentials itself. See
[Local development](/deployment/local/).

## Adding Prometheus

The repository's `docker/prometheus/prometheus.yml` scrapes `felix-broker:8080`
and `felix-controlplane:8080`, the service names used above. Add a service
that mounts it:

```yaml
services:
  prometheus:
    image: docker.io/prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./docker/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus-data:/prometheus
    depends_on:
      - felix-broker

volumes:
  prometheus-data:
```

The file also lists an `otel-collector:8889` target, which stays down unless
you run a collector. The broker exports traces over OTLP when
`OTEL_EXPORTER_OTLP_ENDPOINT` is set; see
[Observability](/features/observability/).

Some queries to start from, at `http://localhost:9090`:

```promql
# Publish rate (a relayed publish is also counted as forwarded)
rate(felix_publish_requests_total{result!="forwarded"}[1m])

# Publish failures, by what went wrong: `error`, `not_owner`, `unroutable`,
# `dropped`. The same counter carries the successes, under `ok` and
# `accepted`; `forwarded` is counted as well when a broker relays a publish.
rate(felix_publish_requests_total{result=~"error|not_owner|unroutable|dropped"}[1m])
```

`felix_publish_latency_ms` exists only in a broker built with
`--features telemetry`. The release images are built without it.
[Observability](/features/observability/) lists the rest of the metrics.

## Configuration

### Environment variables

```yaml
services:
  felix-broker:
    environment:
      # Network
      - FELIX_QUIC_BIND=0.0.0.0:5000
      - FELIX_BROKER_METRICS_BIND=0.0.0.0:8080

      # Control plane
      - FELIX_CONTROLPLANE_URL=http://felix-controlplane:8443
      - FELIX_CONTROLPLANE_SYNC_INTERVAL_MS=2000
      - FELIX_NODE_TOKEN_FILE=/run/secrets/felix-node-token

      # Publishing
      - FELIX_ACK_ON_COMMIT=false
      - FELIX_MAX_FRAME_BYTES=16777216
      - FELIX_PUBLISH_QUEUE_WAIT_MS=2000

      # Event batching
      - FELIX_EVENT_BATCH_MAX_EVENTS=64
      - FELIX_EVENT_BATCH_MAX_BYTES=262144
      - FELIX_EVENT_BATCH_MAX_DELAY_US=250
      - FELIX_FANOUT_BATCH=64

      # Client listener flow control (defaults follow the publish budget)
      - FELIX_BROKER_PUB_CONN_RECV_WINDOW=16777216
      - FELIX_BROKER_PUB_STREAM_RECV_WINDOW=16777216

      - RUST_LOG=info
```

The broker warns at startup about any `FELIX_*` variable it does not read, so a
typo shows up in the logs. The
[environment reference](/reference/environment-variables/) lists them
all.

### Config file

The same kind of settings as YAML:

```yaml
# config/broker.yml
quic_bind: "0.0.0.0:5000"
metrics_bind: "0.0.0.0:8080"
event_batch_max_events: 64
event_batch_max_delay_us: 250
pub_conn_recv_window: 16777216
```

```yaml
services:
  felix-broker:
    volumes:
      - ./config/broker.yml:/etc/felix/broker.yml:ro
    environment:
      - FELIX_BROKER_CONFIG=/etc/felix/broker.yml
```

The file is applied over the environment. An unknown key fails startup.

## More than one broker

A broker that joins a cluster needs more than the single broker above:

- `FELIX_NODE_ID`, unique per broker.
- `FELIX_NODE_ADVERTISE_ADDR`, the internal address other brokers dial, as
  `IP:port`. A hostname is refused, so under Compose each broker needs a
  fixed IP on the network.
- `FELIX_INTERNAL_BIND` for the broker-to-broker listener (default
  `0.0.0.0:5001`).
- Peer mTLS (`FELIX_INTERNAL_TLS_CERT`, `FELIX_INTERNAL_TLS_KEY`,
  `FELIX_INTERNAL_TLS_CA`), or `FELIX_INTERNAL_ALLOW_UNAUTHENTICATED=true`
  when only brokers can reach the internal port. A broker with a node id
  refuses to start with neither.
- A credential that may also register the node (`node.manage`).
- `FELIX_CLIENT_ADVERTISE_ADDR`, the address clients are sent to, when that
  differs from the bind address.

That is a lot to write by hand in Compose. For a local cluster, `task
cluster:up` starts a control plane and three brokers wired this way (see
[Local development](/deployment/local/)). For a real one, the
[Helm chart](/deployment/kubernetes/) renders all of it.

## Persistence

`FELIX_DURABLE_STORAGE_DIR=/var/lib/felix` with a volume at that path, as in
the example above, is all persistence needs. Logs go to stdout. To use a host
directory instead of a named volume, make it writable by uid 65532 first:

```bash
sudo chown 65532:65532 /path/to/host/data
# Rootless Podman maps container uids, so change it from inside its namespace:
podman unshare chown 65532:65532 /path/to/host/data
```

```yaml
services:
  felix-broker:
    volumes:
      - /path/to/host/data:/var/lib/felix
```

### Backups

A tar of a running broker's volume is not a consistent backup: it is taken at
a different moment from every other broker's, and it can hold records no
majority acknowledged. Take a backup point and copy the leaders' shard
directories against it instead; see
[Backup and restore](/deployment/backup-and-restore/). A tar of a
*stopped* single broker's volume is fine.

## Networking

Compose puts every service on one bridge network, where services reach each
other by service name. Clients outside Docker reach the broker on the
published UDP port.

Host networking avoids Docker's port forwarding:

```yaml
services:
  felix-broker:
    network_mode: host
    environment:
      - FELIX_QUIC_BIND=0.0.0.0:5000
```

:::caution[Host networking]
Host networking doesn't work on Docker Desktop or `podman machine` on Mac or Windows. Use bridge networking there, or run on Linux.
:::

## Resource limits

```yaml
services:
  felix-broker:
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 4G
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
```

## Health checks

The images already probe `/ready`. To change the timing:

```yaml
services:
  felix-broker:
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://127.0.0.1:8080/ready"]
      interval: 10s
      timeout: 2s
      retries: 6
      start_period: 10s
```

Use `/ready`, not `/live`. `/ready` turns 503 when the broker starts draining,
while `/live` keeps answering until the process exits.

## Troubleshooting

### The broker exits at startup

```bash
docker compose logs felix-broker
```

The last line says why. `FELIX_CONTROLPLANE_URL must be set for auth` means
the variable is missing. With a bind mount, check that uid 65532 can write it.

### The broker never turns healthy

It has not synced its catalog. Check that the control plane is reachable from
the broker's container and that the credential file holds a valid token:

```bash
docker compose exec felix-broker wget -qO- http://felix-controlplane:8443/v1/system/live
docker compose logs felix-broker | grep -i -e warn -e error
```

### Port conflicts

`port is already allocated` means another process holds a host port. Change
the host side of the mapping. Clients dial the broker's other listeners on the
ports it advertises, which are its container ports, so a remapped broker needs
one listener:

```yaml
environment:
  - FELIX_QUIC_LISTENERS=1
ports:
  - "5001:5000/udp"
  - "8081:8080"
```

### Inspecting a container

```bash
docker compose exec felix-broker /bin/sh
docker compose top felix-broker
docker stats
docker inspect "$(docker compose ps -q felix-broker)"
```

## Next steps

- **Production deployment**: [Kubernetes Guide](/deployment/kubernetes/)
- **Performance tuning**: [Performance Guide](/features/performance/)
- **Full configuration reference**: [Configuration Reference](/reference/configuration/)
- **Monitoring setup**: [Observability Guide](/features/observability/)
