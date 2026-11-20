# The failover checker, as a container. Built from the repository root:
#   docker build -f demos/failover/checker.Dockerfile .
# The compose file does this for you.
ARG BASE_REGISTRY=docker.io/library

FROM ${BASE_REGISTRY}/rust:1.99-bookworm AS build
WORKDIR /felix
COPY . .
RUN --mount=type=cache,id=felix-failover-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=felix-failover-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=felix-failover-target,target=/felix/target \
    cargo build --release --locked --manifest-path demos/failover/Cargo.toml \
    && cp target/release/failover-check /usr/local/bin/failover-check

FROM ${BASE_REGISTRY}/debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates openssl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir /out && chown 65532:65532 /out
COPY --from=build /usr/local/bin/failover-check /usr/local/bin/failover-check
USER 65532:65532
ENTRYPOINT ["failover-check"]
