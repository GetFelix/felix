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

Full documentation: https://docs.getfelix.dev/clients/python/

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

Both surfaces also cover consumer groups (`group_poll`, the settles including
`group_nack_after` and `group_dead_letter`, `group_extend`, and `group_create`,
`group_seek`, `group_describe` and `group_delete`, each with a `*_stream` form
for every shard, sent to the shard's leader by following the broker's redirect;
`group_sharded` polls every shard through one handle),
cache watches (`watch_cache`, including retained watches that hand you current
state before live changes, and `watch_cache_sharded`, which merges a prefix
across every shard), and multi-shard subscriptions (`subscribe_sharded`, which
opens one subscription per shard and merges them; `subscribe(shard=)` reads one
named shard). `offset_for_time` finds the offset a time begins at, and `read`
returns a page of committed records without subscribing. `publish_batch` and
`publish_records` send ordered batches; the latter splits keyed records by
shard and returns a `BatchReport` saying which pieces landed. Two things catch
people out, so they are worth saying here:

- **Publishing without a `key=` puts every record on shard 0.** The key is what
  spreads a stream, and a multi-shard stream published without one behaves
  exactly like a single-shard one.
- **`watch_cache` with a prefix reads one shard.** Keys sharing a prefix do not
  share a shard, so watching a whole multi-shard cache means
  `watch_cache_sharded`.

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

## Tokens that expire

A token lasts 15 minutes by default, and a client that outlives one passes
`token_provider`, a function that returns a fresh token. The client calls it
lazily, one call at a time, when a stream is about to authenticate and the held
token is missing, past two thirds of its life, or was refused. `Client` takes a
plain function or an `async def`; `AsyncClient` takes an `async def`. `token=`
is then optional. See [Getting and refreshing a
token](https://docs.getfelix.dev/clients/python/#getting-and-refreshing-a-token).

```python
felix.Client(addr, tenant_id="t1", token_provider=fetch_token, ca_file=ca)
```

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

## What is not wrapped

The Rust client has more than this binding wraps. What is left out, and why:

- **Subscription counters.** `ClusterSubscription::moves`, `reconnects` and
  `stream_deleted` have no accessor on a subscription, sharded or not. A read
  in flight holds the handle, so these need their own shared state. A
  follow-up is planned; a move still shows up as a `ShardMoved` item on a
  sharded subscription. A cache watch does expose `moves` and `resume_from`.
- **The single-connection `Client` and `Publisher` layer** (`topology`,
  `stream_routing`, `shard_owners`, `inspect_shard`, `supports_*`,
  `publish_batch_binary*`, `connect_with_transport`). The binding wraps
  `ClusterClient`, which owns those connections and runs the feature checks
  itself.
- **`hand_off_*` and `Handed` on the idempotent producer.** They hand back raw
  futures; `publish_batches` is the iterator-shaped equivalent.
- **`ClientConfig` options** such as `publishers` and `timestamps`. They are
  connection configuration, set once, not calls.
- **Reconnect backoff.** `ReconnectPolicy.backoff` and `max_backoff` have no
  option here yet. The client uses their defaults.

## Conformance

The semantics a Felix client must implement are catalogued in
`crates/testing/felix-conformance/scenarios.toml`, with `task conformance:scenarios`
to read them and `task conformance:verify` to check a client's results. That
catalogue — not this README — is the contract, and it is what the next
language's client is gated on.
