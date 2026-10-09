# The control plane, as a container.
#
# Built from the workspace root
# (`docker build -f docker/controlplane.Dockerfile .`). Same two-stage shape as
# the broker image.

# Docker Hub by default. CI passes a mirror of the same official images
# (`--build-arg BASE_REGISTRY=public.ecr.aws/docker/library`) to stay clear
# of Docker Hub's anonymous pull limit. There is no `# syntax=` line for the
# same reason: BuildKit's built-in frontend handles the cache mounts below.
ARG BASE_REGISTRY=docker.io/library

# --- build ---------------------------------------------------------------
FROM ${BASE_REGISTRY}/rust:1.97-bookworm AS build
WORKDIR /felix

COPY . .

ARG BIN=felix-controlplane

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/felix/target \
    cargo build --release --locked -p felix-controlplane-service --bin "${BIN}" \
    && strip "target/release/${BIN}" \
    && cp "target/release/${BIN}" /usr/local/bin/felix-controlplane

# --- runtime -------------------------------------------------------------
FROM ${BASE_REGISTRY}/debian:bookworm-slim AS runtime

# ca-certificates: two reasons, not one — the control plane fetches an IdP's
#   JWKS over TLS, and it connects to Postgres, which is usually TLS too.
# tini: see the broker image.
# wget: the healthcheck below.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini wget \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 65532 felix \
    && useradd --uid 65532 --gid 65532 --home-dir /var/lib/felix --create-home felix

COPY --from=build /usr/local/bin/felix-controlplane /usr/local/bin/felix-controlplane

# No VOLUME: the control plane's state lives in Postgres, or in a Raft log
# directory an operator names explicitly. Declaring one would imply a default
# path it does not have.
WORKDIR /var/lib/felix

USER 65532:65532

# The API (FELIX_CONTROLPLANE_BIND) and metrics
# (FELIX_CONTROLPLANE_METRICS_BIND).
EXPOSE 8443/tcp 8080/tcp

# The readiness probe, which is the one that answers 503 when the database is
# unreachable. `/v1/system/live` would answer 200 through exactly that outage.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=6 \
    CMD wget -qO- http://127.0.0.1:8080/ready >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/felix-controlplane"]
