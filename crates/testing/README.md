# Testing

Tools for exercising Felix rather than running it. Only `felix-loadgen` is
publishable, because `felixctl bench` links it.

- [`felix-cluster`](felix-cluster) starts a real multi-node cluster on one
  machine: an in-process control plane and several `felix-broker` processes. It
  is both a library for the cluster integration tests and a CLI
  (`task cluster:up`). It runs the *prebuilt* broker binary, so build that first
  (`cargo build -p felix-broker-service --bin felix-broker`).
- [`felix-conformance`](felix-conformance) is the client conformance kit: the
  scenario catalogue and the verifier that checks a client's results against
  it. AGPL-3.0-only.
- [`felix-sim`](felix-sim) is the deterministic simulator: brokers in one
  process on tokio's virtual time, over an in-memory peer network with seeded
  faults. It holds the network, the history checkers and an in-memory
  filesystem with a power-loss model; see
  [docs/simulation.md](../../docs/simulation.md).
- [`felix-loadgen`](felix-loadgen) drives a remote cluster for the
  real-network performance suite. Its scenarios are also a library, which
  `felixctl bench` runs.
