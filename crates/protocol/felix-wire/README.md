# felix-wire

The [Felix](https://github.com/GetFelix/felix) wire protocol: frame layout, codec,
and the test vectors both ends are checked against.

Useful on its own if you are writing a Felix client in Rust, or reading frames
off the wire to debug one.

## Capability negotiation, not versioning

Frame flags select the *payload layout*, so an unknown flag bit is rejected
rather than masked off — masking one means confidently misparsing the body.

New features arrive as negotiated flag bits rather than version bumps: a client
offers `Auth.client_flags` and the broker answers with `AuthOk.server_flags`. A
peer that predates negotiation sends a plain `Ok`, and the only safe reading of
that silence is `ORIGINAL_V1_FLAGS` — which is frozen, and never added to.

Optional fields default to the pre-existing behaviour, so an old peer and a new
peer exchange byte-identical frames.

## Documentation

- [Wire protocol reference](https://getfelix.github.io/felix/architecture/wire-protocol/)

Apache-2.0.
