# Panic and abort audit: broker data path

Audit record for [#140](https://github.com/gabloe/felix/issues/140), covering the
`unsafe impl Send` question and the `unwrap()`/`expect()` triage. The sustained-load
and resource-leak portions of that issue are **not** covered here; they were split
into [#154](https://github.com/gabloe/felix/issues/154).

## Scope and environment

- Commit: branch `users/gabloe/remainingm0`.
- Crates in scope: `crates/protocol/felix-wire`, `crates/server/felix-broker`, `crates/protocol/felix-transport`,
  `crates/server/felix-storage`, `services/felix-broker-service`, `services/felix-controlplane-service`.
- Method: static inventory of `unwrap`/`expect`/`panic!`/`unreachable!`/`todo!`/
  `unimplemented!` in non-test code, plus manual review of the binary frame decoders
  for panics that carry no `unwrap` at all (slicing, `Buf` scalar reads, and
  capacity-driven allocation).
- Default (`--no-default-features`-equivalent) builds are the baseline. The
  `telemetry` feature is opt-in and is called out separately.

## `unsafe impl Send`

Both `unsafe impl Send` blocks, on `Broker` and `EphemeralCache`, were removed. Neither
was doing anything: every field of both types is already `Send + Sync`
(`RwLock<HashMap<..>>`, atomics, `usize`, and `Box<dyn StorageApi + Send>` where
`StorageApi: Debug + Send + Sync` supplies both auto traits to the trait object). The
compiler's auto-impls already applied, so the `unsafe` was a no-op that suppressed
nothing and asserted nothing.

Each type now carries a `const _` assertion that it is `Send + Sync`. If a future field
breaks the property, the build fails at the definition rather than surfacing as a
trait-bound error at a distant call site, which is the pressure that produces an
`unsafe impl` "fix" in the first place.

## Panic inventory

198 non-test sites total. Classification:

| Class | Count | Disposition |
| --- | ---: | --- |
| `telemetry` feature only (`timings_telemetry.rs` × 3) | 103 | Not in default builds; mutex-poisoning `expect`s in an opt-in diagnostic path |
| Test modules in non-`tests/` files (`tests.rs`, `bench_ts.rs`) | 68 | Test-only |
| Invariant-protected, request path | 14 | Sound; see below |
| Startup / configuration fail-fast | 8 | Intentional: bad config should not produce a half-live process |
| Infallible conversions | 3 | Sound; see below |
| **Request-reachable, unguarded** | **2** | **Fixed; see findings** |

### Invariant-protected (verified, not assumed)

- `handlers/publish/control.rs`: 12 × `request_id.expect("request id checked")`. The protocol
  invariant is enforced earlier in `handle_publish_message`: an acked publish missing
  `request_id` is rejected with `missing request_id for acked publish` before any of
  these sites run. A malformed client frame produces an error response, not a panic.
- `handlers/publish/ingress.rs`: 2 × `unreachable!("Wait handled above")`. `EnqueuePolicy` is
  config-derived, not request-derived, and `Wait` is handled in an earlier branch.
- `handlers/subscribe/writer.rs`: `frames.pop().expect("single frame")` is guarded by an
  enclosing `frames.len() == 1`.
- `felix-broker/src/stream/state.rs`: `next_seq.checked_add(1).expect("log sequence overflow")`
  requires 2^64 publishes to a single stream.
- `felix-wire`: `encode_slice(..).expect("base64 encode slice")` writes into a buffer
  resized to exactly `base64_len(payload.len())?`, and that helper returns `Err` on
  overflow. This is on the **encode** path, not decode.
- `felix-transport:349`: `u64::try_from(connection.stable_id())` converts from `usize`.

## Findings

### F1: Unbounded allocation from an attacker-declared payload count (fixed)

`decode_publish_batch`, `decode_event_batch`, and `decode_shared_event_batch` read a
`u32` payload count straight off the wire and passed it to `Vec::with_capacity` before
validating it against the bytes actually present. Per-payload validation inside the loop
was correct, but it ran too late; the allocation had already happened.

Reachability: `decode_publish_batch` is called from
`handle_binary_publish_batch_control` **before** the `auth_ctx` check, so any peer that
completes a QUIC handshake can reach it. A ~20-byte frame declaring `u32::MAX` payloads
was sufficient.

Measured impact: a single such frame reserves 95 GiB (`Vec<Vec<u8>>`) to 127 GiB
(`Vec<Bytes>`) of address space. Overcommit means one frame typically succeeds, so this
is not a single-shot crash. At roughly a thousand concurrent decodes the address space
is exhausted and the failing allocation calls `handle_alloc_error`, which **aborts**.
It does not unwind, so tokio's per-task panic recovery does not contain it. Under a
memory cgroup or strict overcommit, as in a typical container deployment, the threshold
is far lower.

Fix: `checked_payload_count` rejects any count exceeding `remaining / 4`, since every
payload requires at least a 4-byte length prefix. Regression tests cover all three
decoders plus a boundary case at the maximum supportable count.

### F2: Non-finding: raw `unwrap` count

The issue notes a ~560-site raw count. The non-test figure is 198, and 171 of those are
test modules in non-`tests/` files or the opt-in `telemetry` feature. The count on its
own does not indicate risk, and, as F1 shows, the most serious defect in the decode
path involved no `unwrap` at all. Counting `unwrap` would not have found it.

## Residual risk

- The `telemetry` feature's 103 mutex-poisoning `expect`s are unreviewed. They are not
  in default builds; if `telemetry` is ever enabled in production, they need their own
  pass.
- The frame and message decoders are fuzzed (`crates/protocol/felix-wire/fuzz/`, four
  targets), as are the Kafka listener's request and record-batch decoders
  (`crates/server/felix-kafka/fuzz/`), plus six storage targets covering segments, the
  sparse index, cache and counter records, and the per-shard state files. CI runs each
  target for 30 s on every PR as a regression gate, and `fuzz-nightly.yml` runs each for
  25 minutes a night from a corpus that carries over between runs.
- Sustained-load, connection-churn, and resource-leak evidence is not covered by this
  document; it is tracked in [#154](https://github.com/gabloe/felix/issues/154). Until
  that lands, M0's "no known concurrency or leak issues" criterion rests on this static
  audit alone.
