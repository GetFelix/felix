# syntax=docker/dockerfile:1.7
# The broker, as a container.
#
# Built from the workspace root (`docker build -f docker/broker.Dockerfile .`),
# because the binary is a workspace member and cargo needs the workspace to
# resolve it. `.dockerignore` is what keeps that from meaning a 16GB context.
#
# Two stages: one that has a Rust toolchain, and one that does not. The runtime
# image carries the binary, a CA bundle, and an init — no compiler, no package
# manager, nothing else for an attacker to find.

# --- build ---------------------------------------------------------------
FROM rust:1.97-bookworm AS build
WORKDIR /felix

# Everything, rather than a manifest-first dependency-caching dance. That trick
# needs every workspace member's manifest listed by hand, and a missed one fails
# confusingly — which is how the previous version of this file ended up
# referencing `crates/broker`, a path that has not existed since the workspace
# reorganisation. BuildKit's cache mounts below get most of the same benefit
# with none of the bookkeeping.
COPY . .

ARG BIN=felix-broker

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/felix/target \
    cargo build --release --locked -p felix-broker-service --bin "${BIN}" \
    && strip "target/release/${BIN}" \
    # Copied out of the cache mount, which does not survive the layer.
    && cp "target/release/${BIN}" /usr/local/bin/felix-broker

# --- runtime -------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# ca-certificates: QUIC is TLS 1.3 only and the broker verifies the control
#   plane's certificate, so the trust store is load-bearing.
# tini: a real init at PID 1. The broker handles SIGTERM itself, but PID 1 on
#   Linux gets no default signal handlers and no zombie reaping, and a missed
#   SIGTERM means every rolling update ends in a kill.
# wget: the healthcheck below. debian-slim ships neither wget nor curl, which
#   is why the previous healthcheck here could never have passed.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini wget \
    && rm -rf /var/lib/apt/lists/*

# A fixed uid, so a Kubernetes `runAsUser` and a volume's ownership can be set
# without inspecting the image first.
RUN groupadd --gid 65532 felix \
    && useradd --uid 65532 --gid 65532 --home-dir /var/lib/felix --create-home felix

COPY --from=build /usr/local/bin/felix-broker /usr/local/bin/felix-broker

# Durability is opt-in — with FELIX_DURABLE_STORAGE_DIR unset the broker keeps
# nothing on disk — so this is the path to point it at rather than one the
# broker already uses. Declared as a volume so an operator who sets that
# variable and forgets the mount fills a volume rather than the container's
# writable layer, which grows until the node evicts the pod.
VOLUME ["/var/lib/felix"]
WORKDIR /var/lib/felix

USER 65532:65532

# Client QUIC (FELIX_QUIC_BIND, up to four ports by default), broker-to-broker
# QUIC (FELIX_INTERNAL_BIND, which a standalone broker does not bind), and
# metrics (FELIX_BROKER_METRICS_BIND). Documentation rather than enforcement —
# EXPOSE publishes nothing on its own — but it is what tooling reads.
EXPOSE 5000-5003/udp 8080/tcp

# `/ready`, not `/healthz`: that is the path the broker actually serves, and it
# is the one that flips during a drain. `/live` stays up on a broker that is
# shutting down correctly, so using it here would keep sending traffic to one.
HEALTHCHECK --interval=10s --timeout=2s --start-period=10s --retries=6 \
    CMD wget -qO- http://127.0.0.1:8080/ready >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/felix-broker"]
