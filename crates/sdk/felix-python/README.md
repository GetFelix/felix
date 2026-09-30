# felix-client for Python

Python bindings for Felix, built as a wrapper over the Rust client rather than
a reimplementation of the protocol.

That distinction is the point. Reconnection, redirect-following, retry
classification and offset bookkeeping are hard to get right and expensive to
get wrong — a second implementation is a second set of subtle bugs in exactly
the places that matter. Here they exist once, in `felix-client`, and every
language binds to them.

```bash
pip install felix-client
```

Wheels ship compiled, one per platform, covering every Python from 3.9 up — so
installing needs no Rust toolchain. Linux (x86-64 and arm64), macOS (Intel and
Apple silicon) and Windows x86-64 are covered; anything else builds from the
sdist and does need one.

Full documentation: https://gabloe.github.io/felix/clients/python/

## Two surfaces

Synchronous, for threads and `asyncio.to_thread`:

```python
import felix

with felix.Client("127.0.0.1:5000", tenant_id="t1", token=tok, ca_file=ca) as client:
    client.publish("t1", "default", "events", b"hello")
    with client.subscribe("t1", "default", "events") as events:
        for event in events:
            print(event.payload, event.offset)
```

Asynchronous, which is what most Python realtime backends want:

```python
client = await felix.AsyncClient.connect(addr, tenant_id="t1", token=tok, ca_file=ca)
async with client:
    await client.publish("t1", "default", "events", b"hello")
    async with await client.subscribe("t1", "default", "events") as events:
        async for event in events:
            print(event.payload, event.offset)
```

Both wrap the same client and fail over identically. The sync one blocks with
the GIL released; the async one yields to your event loop.

`publish` returns the record's log offset once the broker has written it, and
`None` when it acknowledged earlier than that (`ack="none"`, or a broker that
acks on enqueue), the stream has no log, or the broker is too old to say.

## Beyond publish and subscribe

Both surfaces also cover consumer groups (`group_poll` and the four settles,
sent to the shard's leader by following the broker's redirect),
cache watches (`watch_cache`, including retained watches that hand you current
state before live changes), and multi-shard subscriptions (`subscribe_sharded`,
which opens one subscription per shard and merges them). Two things catch
people out, so they are worth saying here:

- **Publishing without a `key=` puts every record on shard 0.** The key is what
  spreads a stream, and a multi-shard stream published without one behaves
  exactly like a single-shard one.
- **A prefix watch reads one shard.** Keys sharing a prefix do not share a
  shard, so watching a whole multi-shard cache means one watch per shard.

Both surfaces also have atomic commits: `commit(tenant_id, namespace,
entity_key, [CommitOp.publish(...) or CommitOp.enqueue(...), CommitOp.put(...),
CommitOp.delete(...)])` writes an event and its state changes as one record and
returns a `CommitReceipt` with the offset, and `state_get` reads the state back
with that offset as its version. A commit naming a second stream raises
`NotOnOwningShardError`, and one without exactly one event raises
`EventCountError`, before anything is sent. See
[atomic commits](../../../docs/atomic-commit.md) for what atomic covers.

The [clients page](../../../docs-site/src/content/docs/api/clients.md) has the
worked examples.

## TLS

QUIC has no unencrypted mode, so there is always a trust decision — and
deliberately no "skip verification" switch, because that is the setting that
silently turns a secure deployment insecure. Either the platform trust store
(the default) or an explicit CA:

```python
felix.Client(addr, tenant_id=..., token=..., ca_file="/path/to/ca.pem")
```

A development broker generates a self-signed certificate at startup; set
`FELIX_TLS_CERT_EXPORT=/path/ca.pem` on the broker and point `ca_file` at it.

## Building

```bash
pip install maturin
task python:develop      # build and install into the active virtualenv
task python:check        # fmt, clippy, build
```

This crate is deliberately outside the Cargo workspace — an extension module
carries `pyo3/extension-module`, which cannot be linked into a test binary —
so `task python:check` is what builds it, not `task test`.

## Conformance

The semantics a Felix client must implement are catalogued in
`crates/testing/felix-conformance/scenarios.toml`, with `task conformance:scenarios`
to read them and `task conformance:verify` to check a client's results. That
catalogue — not this README — is the contract, and it is what the next
language's client is gated on.
