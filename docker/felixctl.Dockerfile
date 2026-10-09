# felixctl, as a container.
#
# Built from the workspace root (`docker build -f docker/felixctl.Dockerfile .`).
# Same two-stage shape as the broker image, minus what a short-lived CLI does
# not need: no init, no healthcheck, no ports.

# Docker Hub by default. CI passes a mirror of the same official images
# (`--build-arg BASE_REGISTRY=public.ecr.aws/docker/library`) to stay clear
# of Docker Hub's anonymous pull limit. There is no `# syntax=` line for the
# same reason: BuildKit's built-in frontend handles the cache mounts below.
ARG BASE_REGISTRY=docker.io/library

# --- build ---------------------------------------------------------------
FROM ${BASE_REGISTRY}/rust:1.97-bookworm AS build
WORKDIR /felix

COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/felix/target \
    cargo build --release --locked -p felixctl --bin felixctl \
    && strip target/release/felixctl \
    && cp target/release/felixctl /usr/local/bin/felixctl

# --- runtime -------------------------------------------------------------
FROM ${BASE_REGISTRY}/debian:bookworm-slim AS runtime

# ca-certificates: felixctl verifies brokers and the control plane against the
# system trust store unless a context names its own CA file.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 65532 felix \
    && useradd --uid 65532 --gid 65532 --home-dir /home/felix --create-home felix

COPY --from=build /usr/local/bin/felixctl /usr/local/bin/felixctl

# Contexts live in $HOME/.config/felixctl/config.toml; mount a directory there,
# or pass FELIX_* variables, to give the container a cluster to talk to.
USER 65532:65532
ENV HOME=/home/felix
WORKDIR /home/felix

ENTRYPOINT ["felixctl"]
