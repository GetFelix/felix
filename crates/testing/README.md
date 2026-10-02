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
  it. Apache-2.0, so a third party can vendor it.
- [`felix-loadgen`](felix-loadgen) drives a remote cluster for the
  real-network performance suite. Its scenarios are also a library, which
  `felixctl bench` runs.
