# syntax=docker/dockerfile:1.7
# felixctl, as a container.
#
# Built from the workspace root (`docker build -f docker/felixctl.Dockerfile .`).
# Same two-stage shape as the broker image, minus what a short-lived CLI does
# not need: no init, no healthcheck, no ports.

# --- build ---------------------------------------------------------------
FROM docker.io/library/rust:1.97-bookworm AS build
WORKDIR /felix

COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/felix/target \
    cargo build --release --locked -p felixctl --bin felixctl \
    && strip target/release/felixctl \
    && cp target/release/felixctl /usr/local/bin/felixctl

# --- runtime -------------------------------------------------------------
FROM docker.io/library/debian:bookworm-slim AS runtime

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
