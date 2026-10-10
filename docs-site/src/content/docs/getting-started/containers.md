---
title: "Docker or Podman"
description: "Run Felix's images, compose files and container-backed tasks with either Docker or Podman, and where the two differ."
---

Everything in these docs that runs a container works with Docker or Podman.
The commands are written with `docker`. With Podman, replace `docker` with
`podman`; this page lists the places where that is not enough.

## Setup

**Docker.** Install Docker Engine on Linux or Docker Desktop on macOS and
Windows. `docker version` should print both a client and a server section.

**Podman.** Install Podman 5 or later. On Linux it runs containers directly,
rootless by default. On macOS and Windows it runs them in a VM that has to
exist and be running:

```bash
podman machine init      # once
podman machine start     # after every reboot
podman version           # prints a server section once the machine is up
```

On macOS the machine shares `$HOME`, `/private` and `/var/folders` with the VM
by default. A checkout or a mounted file anywhere else, such as on a volume
under `/Volumes`, is invisible to containers until you add it when creating
the machine. Passing `-v` replaces the defaults, so list them too:

```bash
podman machine init -v "$HOME:$HOME" -v /private:/private \
  -v /var/folders:/var/folders -v /Volumes:/Volumes
```

For Compose, `podman compose` runs an external provider, `docker-compose` or
`podman-compose`, whichever it finds first. Use `docker-compose` 2.20 or later.
The compose files in these docs rely on `depends_on` health conditions and
file secrets, and `podman-compose` names containers differently from Docker
Compose, so commands that look a container up by name behave differently
under it. `podman compose version` shows which provider it picked.

## Choosing the engine for tasks

`task test`, `task coverage`, `task pg:up`, `task pg:down`, `task pg:sweep` and
`task tla:check` pick the engine once, in `scripts/container_engine.sh`:
`CONTAINER_ENGINE` if it is set, otherwise the first of `docker` and `podman`
whose `version` succeeds. With neither, they skip the Postgres tests the same
way they always have. To force one:

```bash
CONTAINER_ENGINE=podman task test
```

`task test` passes the choice on to the tests that start a `kcat` container
(the Kafka listener tests), which read the same variable and fall back to the
same detection when run with `cargo test` directly.

The control plane's Postgres tests are the exception when run without `task`.
Given no `FELIX_TEST_DATABASE_URL`, they start Postgres through the
testcontainers library, which runs the `docker` CLI and nothing else. With
Podman, start the database first and point the tests at it:

```bash
task pg:up
FELIX_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55432/postgres \
  cargo test -p felix-controlplane-service --features pg-tests
task pg:down
```

## Where Podman differs

| Area | Docker | Podman |
|------|--------|--------|
| Image names | `postgres:16-alpine` resolves to Docker Hub | A short name can be refused or ambiguous, depending on `unqualified-search-registries` in `registries.conf`. The repository's Dockerfiles, compose file and tasks use fully qualified names (`docker.io/library/postgres:16-alpine`), which work in both. Do the same in your own files. |
| Reaching the host from a container | `host.docker.internal` | `host.containers.internal` |
| Compose | `docker compose` | `podman compose`, with `docker-compose` 2.20+ as the provider (see above) |
| Building the images | `docker build` keeps the `HEALTHCHECK` | `podman build` writes OCI images by default, which drop `HEALTHCHECK`. Pass `--format docker` so `service_healthy` and the image's `/ready` probe still work. |
| Bind-mounting the broker's storage | `chown 65532:65532` the host directory | Rootless, uid 65532 in the container is a different uid on the host. Use `podman unshare chown 65532:65532 /path/to/data`. |
| Running `felixctl` with your config mounted | `--user "$(id -u):$(id -g)"` | Rootless, use `--userns=keep-id` instead, so the container runs as your uid. |
| SELinux hosts (Fedora, RHEL) | Same as Podman when SELinux is enforcing | Add `:z` to bind mounts (`-v ./prometheus.yml:/etc/prometheus/prometheus.yml:ro,z`) or the container cannot read them. |
| Host ports below 1024 | Allowed | Rootless Podman on Linux cannot bind them unless `net.ipv4.ip_unprivileged_port_start` is lowered. Felix's default ports are all above 1024. |
| `--network host` / `network_mode: host` | Linux only; on Docker Desktop it does not reach the Mac or Windows host | The same: Linux only; under `podman machine` it is the VM's network. |

## Felix commands, side by side

```bash
# The control plane over the Postgres from `task pg:up`, as in Installation
podman run -p 8443:8443 \
  -e FELIX_CONTROLPLANE_POSTGRES_URL=postgres://postgres:postgres@host.containers.internal:55432/postgres \
  ghcr.io/getfelix/felix-controlplane:0.6.0-preview.5

# Build the broker image, keeping its health check
podman build --format docker -t felix-broker -f docker/broker.Dockerfile .

# The Compose example
podman compose up -d
podman compose logs -f felix-broker
```
