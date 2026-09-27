# felix-conformance

The Felix client conformance kit: a catalogue of scenarios every client must
pass, and a verifier for a client's results.

```bash
cargo run -p felix-conformance                        # protocol suite against a real broker over QUIC
cargo run -p felix-conformance -- scenarios           # print the catalogue
cargo run -p felix-conformance -- verify results.json # check a client's results
```

With no arguments it drives a broker over QUIC and checks publish, subscribe and
cache behaviour against [`docs/protocol.md`](../../../docs/protocol.md). The
catalogue and `verify` are how the Python and TypeScript clients, and any third
party's, show they behave like the Rust client. `felix-cluster client-fixture`
starts something to run a client against. Byte-level wire fixtures are separate:
they live in [`felix-wire`'s `tests/vectors/`](../../protocol/felix-wire/tests/vectors).

## Licensing

This crate is AGPL-3.0-only. It links AGPL-3.0 crates (`felix-broker-service`,
`felix-broker`, `felix-storage`, `felix-authz`) to run its suite against the
reference broker, so any build of it is AGPL regardless of the label;
`scripts/check_license_graph.py` fails CI if the label says otherwise. See
[`LICENSING.md`](../../../LICENSING.md).

Running it to verify your client's results puts no obligation on your client:
the verifier reads a results file, it is not linked into what you ship. The
protocol it checks is specified in [`docs/protocol.md`](../../../docs/protocol.md)
and the Apache-2.0 `felix-wire` test vectors.

It is marked `publish = false`: it is a test harness rather than something to
depend on from a registry.
