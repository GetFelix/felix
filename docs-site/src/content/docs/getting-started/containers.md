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

`task tla:check` picks the engine once, in `scripts/container_engine.sh`:
`CONTAINER_ENGINE` if it is set, otherwise the first of `docker` and `podman`
whose `version` succeeds. To force one:

```bash
CONTAINER_ENGINE=podman task tla:check
```

The tests that start a `kcat` container (the Kafka listener tests) read the
same variable and fall back to the same detection.

## Where Podman differs

| Area | Docker | Podman |
|------|--------|--------|
| Image names | `eclipse-temurin:21-jre` resolves to Docker Hub | A short name can be refused or ambiguous, depending on `unqualified-search-registries` in `registries.conf`. The repository's Dockerfiles, compose file and scripts use fully qualified names (`ghcr.io/getfelix/felix-broker`, `docker.io/library/eclipse-temurin:21-jre`), which work in both. Do the same in your own files. |
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
# Build the broker image, keeping its health check
podman build --format docker -t felix-broker -f docker/broker.Dockerfile .

# The one-command stack (compose.yaml at the repository root)
export PODMAN_COMPOSE_PROVIDER=docker-compose
podman compose up
podman compose run --rm felixctl pub orders 'hello'
```

`compose.yaml` uses fully qualified image names and declares its own health
checks, so it runs the same under both, given the `docker-compose` provider.
Under `podman-compose` it does not: `run` cannot start the profiled `felixctl`
service, and the broker starts even if `felix-dev-auth` failed. Its smoke test,
`scripts/ci/compose_smoke.sh`, takes `CONTAINER_ENGINE=podman`.

## Where next

- [Quickstart](/getting-started/quickstart/): the full local stack
- [Docker Compose](/deployment/docker-compose/): a compose deployment in depth
- [Kubernetes](/deployment/kubernetes/): the Helm chart
