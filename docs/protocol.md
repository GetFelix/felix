# Felix Wire Protocol (v1)

This document defines the language-neutral wire format for Felix. It is the
source of truth for all client implementations.

> Brokers speak a separate protocol to each other, with its own magic, version
> and message kinds. See [the broker-internal forwarding protocol](internal-protocol.md).

## Goals
- Stable, versioned envelope
- Minimal message set for v1
- No Rust-specific semantics
- Simple framing over QUIC (and future TCP+TLS)

## Transport
- QUIC over TLS 1.3 (IETF QUIC)
- Client-opened bidirectional streams carry request/response traffic: `auth`,
  publishes, cache and counter operations, group polls, and subscribe setup.
  Each stream authenticates on its own with `auth` before any other request.
- Broker-opened unidirectional streams carry events. The broker opens one per
  subscription (and per cache watch), starting with `event_stream_hello`.
- A client MAY also open a unidirectional stream for publish-only traffic. It
  must begin with `auth`, may carry only publishes, and the broker never replies
  on it; anything else closes the stream.

Before authentication. Every client stream starts with an `auth`, and until that
succeeds the broker limits what the stream may cost it:

- A frame on an unauthenticated stream may carry at most `preauth_max_frame_bytes`
  (64 KiB by default) of payload. A larger `length` ends the stream on the header
  alone.
- A connection that has authenticated no stream within `auth_timeout_ms`
  (10 s by default) is closed with QUIC application error code `1` and reason
  `authentication timeout`. A client that dials a connection it may not use for a
  while should authenticate a stream on it straight away.
- Connections past `max_client_connections` are refused during the QUIC
  handshake (`CONNECTION_REFUSED`).

Client connection close codes: `0` broker shutting down, `1` authentication
timeout.

## Frame Envelope
All messages are sent in a fixed header + payload frame.

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+---------------------------------------------------------------+
|                          magic (u32)                          |
+-------------------------------+-------------------------------+
|         version (u16)         |          flags (u16)          |
+-------------------------------+-------------------------------+
|                          length (u32)                         |
+---------------------------------------------------------------+
```

Each row above is 32 bits, so the 12-byte header occupies three rows.

| Offset | Size | Field | Type | Value |
| --- | --- | --- | --- | --- |
| 0 | 4 | `magic` | u32 | `0x464C5831` (`"FLX1"`) |
| 4 | 2 | `version` | u16 | `1` |
| 6 | 2 | `flags` | u16 | Bit field; see below |
| 8 | 4 | `length` | u32 | Payload length in bytes |

Field definitions:
- `magic` (u32, big-endian): `0x464C5831` ("FLX1")
- `version` (u16, big-endian): `1`
- `flags` (u16, big-endian): selects the payload layout. `0` means the payload is
  a JSON-encoded `Message`. Defined bits:

  | Bit | Name | Meaning |
  | --- | --- | --- |
  | `0x0001` | `BINARY_PUBLISH_BATCH` | Payload is a binary publish batch |
  | `0x0002` | `BINARY_EVENT_BATCH` | Payload is a binary event batch (legacy, per-subscriber) |
  | `0x0004` | `BINARY_EVENT_BATCH_SHARED` | Payload is a shared binary event batch |
  | `0x0008` | `BINARY_PUBLISH_ACKED` | Modifier on `0x0001`: the batch carries a `request_id` prefix and is owed an ack |
  | `0x0010` | `BINARY_PUBLISH_ACK` | Payload is a binary publish acknowledgement (broker → client) |
  | `0x0020` | `EVENT_BATCH_OFFSETS` | Modifier on `0x0002` or `0x0004`: the batch carries a `base_offset` |
  | `0x0040` | `BINARY_PUBLISH_KEYED` | Modifier on `0x0001`: the batch carries a routing key prefix |
  | `0x0080` | `BINARY_PUBLISH_ACK_OWNER` | Modifier on `0x0010`: the batch was forwarded, and the ack names the shard's owner |
  | `0x0100` | `BINARY_PUBLISH_IDEMPOTENT` | Modifier on `0x0008`: the batch carries an idempotent producer's id and sequence |
  | `0x0200` | `BINARY_PUBLISH_ACK_CODE` | Modifier on `0x0010`: a failed ack carries an error code and retry class |
  | `0x0400` | `BINARY_PUBLISH_ACK_DETAIL` | Modifier on `0x0200`: the code is followed by the error's `detail` (reason, suggested wait) |
  | `0x0800` | `EVENT_BATCH_SKIPPED` | Modifier on `0x0020`: the batch also carries a `skipped_before` count of offsets before it that hold no event |

  Because these bits change how the payload is parsed, a receiver MUST reject a
  frame carrying any bit it does not recognise rather than masking it off — see
  Future Compatibility.
- `length` (u32, big-endian): payload length in bytes

Payload:
- With `flags = 0`, the payload is a UTF-8 JSON object encoding a `Message` (see below).
  The binary layouts selected by the flag bits are described in their own sections.
- Encoders MUST NOT exceed `u32::MAX` bytes.

## Message Types (v1)
Message schemas below are the JSON objects carried in a `flags = 0` frame. Byte
fields such as `payload` are base64 strings.

### Publish / PublishBatch (compatibility only)
```
{ "type": "publish", "tenant_id": "<string>", "namespace": "<string>", "stream": "<string>", "payload": "<base64>", "ack": "<none|per_message>" }
{ "type": "publish_batch", "tenant_id": "<string>", "namespace": "<string>", "stream": "<string>", "payloads": ["<base64>", ...], "ack": "<none|per_batch>" }
```

**These are not the data path.** A publish travels as a binary frame — see
[Binary PublishBatch](#binary-publishbatch) and
[Binary keyed PublishBatch](#binary-keyed-publishbatch) — and a routing key has
ridden in that frame since `0x0040`. The JSON forms measured **645.8 MB/s
against 917** for the same keyed workload on the same rig, with user CPU up from
20% to 28%, and they now buy nothing the binary frames do not cover.

They are still accepted, and will be: `ORIGINAL_V1_FLAGS` is frozen, so a client
older than `0x0008` or `0x0040` is entitled to keep sending them and must keep
working. What changed is that **no current Felix client emits one except as a
fallback** it chooses itself, against a broker that did not advertise the frame
it wanted. Brokers count what still arrives on this path as
`felix_broker_json_publishes_total{frame="publish"|"publish_batch"}`, which is
the evidence a deployment would need before this arm could ever be dropped.

`publish_idempotent` is unaffected: it is the form a client sends to a broker
that did not advertise `0x0100`, which carries the producer id and sequence in a
binary frame (see [Binary idempotent PublishBatch](#binary-idempotent-publishbatch)).

### ProducerInit
```
{ "type": "producer_init", "request_id": <u64> }
```
Asks the broker for a producer id. Only sent to a broker that advertised
`FEATURE_IDEMPOTENT_PRODUCER`; see [idempotent producers](#idempotent-producers).

### ProducerInitOk (server -> client)
```
{ "type": "producer_init_ok", "request_id": <u64>, "producer_id": <u64> }
```

### PublishIdempotent
```
{ "type": "publish_idempotent", "tenant_id": "<string>", "namespace": "<string>", "stream": "<string>", "payloads": ["<base64>", ...], "key": "<base64, optional>", "request_id": <u64>, "producer_id": <u64>, "sequence": <u64> }
```
A batch the broker appends once however many times it arrives. Always
acknowledged, with `publish_ok` or `publish_refused`. `sequence` counts this
producer's batches on the shard from zero, one per batch whatever its size.

### PublishRefused (server -> client)
```
{ "type": "publish_refused", "request_id": <u64>, "reason": <reason>, "message": "<string>" }
```
Where `reason` is one of `{"sequence_gap": {"expected": <u64>}}`,
`"unknown_producer"`, `"sequence_expired"`, `"sequence_reused"`, or
`{"not_leader": {"node_id": "<string>", "addr": "<host:port, optional>"}}`.
Only ever sent in answer to a `publish_idempotent`, and `"sequence_reused"` only
to a client that offered `FEATURE_SEQUENCE_REUSED`.

### Subscribe
```
{ "type": "subscribe", "tenant_id": "<string>", "namespace": "<string>", "stream": "<string>", "start": <"latest"|"earliest"|{"offset": <number>}> }
```

`start` is optional. Omitting it means `latest`, which is what every client sent
before the field existed — so an old client and a new broker exchange exactly the
frames they always did.

- `latest` — deliver only what is published from now on.
- `earliest` — the oldest record still retained. Deliberately not "offset 0":
  for a stream whose head has been trimmed, offset 0 is gone, and `earliest`
  means "as far back as you can" rather than an error.
- `{"offset": n}` — resume at exactly offset `n`, the first record the client has
  *not* seen. A client that checkpoints the offset it last handled resumes at
  that offset plus one.

Requesting an offset the broker cannot serve returns `subscribe_cursor_error`
rather than a silent restart at the tail:

```
{ "type": "subscribe_cursor_error", "reason": "<too_old|in_future>", "requested": <number>, "available": <number> }
```

`too_old` means retention has discarded the offset and `available` is the oldest
still retained. `in_future` means the offset is past the end of the stream and
`available` is the current tail. The two have opposite remedies, which is why
they are distinguishable in code rather than only in prose.

The `shard` field selects which shard of the stream to read, defaulting to 0. A
subscription reads **one** shard, so consuming a whole multi-shard stream means
one subscription per shard; `stream_shards` says how many there are.

### Subscribed (server -> client)
```
{ "type": "subscribed", "subscription_id": <number>,
  "start_offset": <u64>?, "live_offset": <u64>? }
```

Confirms a subscription and carries the id the broker assigned it. The same id
opens the event stream that carries its deliveries:

```
{ "type": "event_stream_hello", "subscription_id": <number> }
```

which is the first message on the unidirectional stream the broker opens back,
and is how a client matches an event stream to the subscription that asked for
it.

`start_offset` is the first offset the subscription delivers. `live_offset` is
the stream's tail when the subscriber was registered: anything below it was
already in the stream, anything from it on was written after, and nothing falls
between. It leaves out generation-start records the log ends with (they hold an
offset but never an event, so a reader waiting for the raw tail would wait
forever), but is never below `start_offset`. For `latest` the two are equal. Both are sent only for a subscribe
with a `start`, on a durable stream, to a client that negotiated
`FLAG_EVENT_BATCH_OFFSETS`. Otherwise the frame is unchanged.

### Event (server -> client)
```
{ "type": "event", "tenant_id": "<string>", "namespace": "<string>", "stream": "<string>", "payload": "<base64>", "offset": <number|absent> }
```

`offset` is present for durable streams and absent for in-memory ones, which
have no durable position to checkpoint against.

### ShardMoved (server -> client)
```
{ "type": "shard_moved", "subscription_id": <u64>, "resume_from": <u64|absent>, "node_id": "<string|absent>", "addr": "<string|absent>", "generation": <u64> }
```

The last frame on the event stream of a subscription or cache watch whose shard
this broker stopped serving, because the shard moved. Everything already queued
for the reader is delivered first; the broker finishes the stream after this.
Sent only to a client that offered `FEATURE_SHARD_MOVED`: any other client sees
the stream end after its last event, byte for byte as before. See
[Shard moves](#shard-moves).

### CachePut
```
{ "type": "cache_put", "key": "<string>", "value": "<base64>", "ttl_ms": <number|null> }
```

### CacheGet
```
{ "type": "cache_get", "key": "<string>" }
```

### GroupPoll
```
{ "type": "group_poll", "tenant_id": "<string>", "namespace": "<string>",
  "stream": "<string>", "shard": <number>, "group": "<string>",
  "max_records": <number>, "wait_ms": <number>, "request_id": <number> }
```

Sent only to a broker that advertised `FEATURE_CONSUMER_GROUP`, and only to the
broker that leads the shard.

`wait_ms` is how long the broker may hold the request open waiting for work.
Omitted or `0` answers immediately — which is what a broker that predates
long-polling does with the field, so an older peer degrades to a plain poll
rather than misreading the request. The broker caps it at
`FELIX_GROUP_MAX_WAIT_MS`, so a client cannot hold a stream open indefinitely.
The wait bounds how long the broker looks, not whether it answers: an empty
`group_records` after the wait still means nothing was available.

The broker hands out at most 1,000 records per poll, and stops early once it has
read about 4 MiB of payload, whatever `max_records` asks for. A group also has
at most `FELIX_GROUP_MAX_IN_FLIGHT` records handed out and unsettled on a shard;
a poll at that cap is answered empty (after its wait) until acknowledgements,
hand-backs or lapsed claims free room.

### GroupRecords (server -> client)
```
{ "type": "group_records",
  "records": [{ "offset": <number>, "payload": "<base64>", "attempts": <number> }],
  "request_id": <number> }
```

`attempts` counts deliveries including this one, so `1` is a first attempt and
anything higher is a redelivery. Absent means the broker did not report it —
which is not the same as a first attempt, and a consumer should not treat it as
one.

### GroupDeadLetters / GroupDiscard / GroupRedrive
```
{ "type": "group_dead_letters", "tenant_id": "...", "namespace": "...",
  "stream": "...", "shard": <number>, "group": "<string>", "request_id": <number> }
{ "type": "group_discard", ..., "offset": <number>, "request_id": <number> }
{ "type": "group_redrive", ..., "offset": <number>, "request_id": <number> }
```

Sent only to a broker that advertised `FEATURE_GROUP_DEAD_LETTERS`.

### GroupDeadLetterList (server -> client)
```
{ "type": "group_dead_letter_list", "offsets": [<number>], "request_id": <number> }
```

### GroupAck / GroupNack
```
{ "type": "group_ack",  "tenant_id": "...", "namespace": "...", "stream": "...",
  "shard": <number>, "group": "<string>", "offset": <number>, "request_id": <number> }
{ "type": "group_nack", ... }
```

An offset at or past the shard's log tail was never handed out and is refused
with `invalid_request`. The group's in-flight state is kept in memory, so
after it is evicted (idle for 10 minutes) or the shard moves or fails over, a
fresh one is built. It takes a settle for any offset below the log tail it
first saw, since a claim made before it could be any of those. An offset
written after that and not yet handed out is refused with `stale_claim`
(`retry`): nothing was applied and the record will be delivered. An offset below the group's position is a harmless
duplicate and answered `ok`.

### CacheDelete
```
{ "type": "cache_delete", "tenant_id": "<string>", "namespace": "<string>",
  "cache": "<string>", "key": "<string>", "request_id": <u64|absent> }
```

Sent only to a broker that advertised `FEATURE_CACHE_DELETE`.

Answered with `cache_value` carrying the value that was removed, or a null value
if the key was not there — so a caller can tell a delete that did something from
one that did not.

### CacheWatch
```
{ "type": "cache_watch", "tenant_id": "<string>", "namespace": "<string>",
  "cache": "<string>", "key": "<string>|absent", "prefix": "<string>|absent",
  "shard": <u32|absent>, "from_offset": <u64|absent>,
  "retained": <bool|absent>, "subscription_id": <u64|absent> }
```

Sent only to a broker that advertised `FEATURE_CACHE_WATCH`. Subscribes to
changes for one cache key (`key`) or key prefix (`prefix`) — exactly one of the
two must be present; both or neither is refused rather than guessed at. An
empty `prefix` is every key in the shard.

A watch reads **one** shard, exactly as a stream subscription does. A `key`
watch resolves its own shard by hashing — the same resolution a `cache_get`
uses — and ignores `shard`. A `prefix` watch reads `shard`, because keys
sharing a prefix hash to different shards; a whole multi-shard cache is one
watch per shard. Absent means 0 on a single-shard cache and is refused with an
`Error` on a multi-shard one: reading shard 0 there would cover only the keys
that hash to it while looking like a complete prefix watch. `cache_shards` says
how many shards to watch.

`from_offset` is where to resume: the first change the client has *not* seen,
so a client checkpoints the offset it last handled plus one. Absent means from
now — live changes only. An offset past the tail is refused with
`subscribe_cursor_error` rather than silently reinterpreted. Watches are served
by the shard's owner and redirected (`not_leader`) elsewhere, like subscribes.

`retained` asks for current state first: each matching key's current value —
MQTT's retained message — then live changes. Sent only to a broker that
advertised `FEATURE_CACHE_WATCH_RETAINED`: an older watch-capable broker would
ignore the unknown field and serve a live-only watch, the client silently
missing exactly the state it joined for. Refused alongside `from_offset` —
the replay already reconstructs the state a retained start shortcuts, and
serving both would hand over every value twice.

### CacheWatchStarted (server -> client)
```
{ "type": "cache_watch_started", "subscription_id": <u64>,
  "resume_offset": <u64>, "resnapshot": <bool|absent>,
  "retained_count": <u64|absent> }
```

Confirms the watch. The same `subscription_id` arrives in the
`event_stream_hello` that opens the unidirectional stream carrying the watch's
changes — the binding is identical to a stream subscription's.

`resume_offset` is the offset live delivery begins at: every change at or past
it is delivered, and everything before it was covered by the replay or the
snapshot. `resnapshot` (absent means false) is true when `from_offset` named
history that compaction has already collapsed; the watch then begins with each
matching key's **current value** instead of the collapsed history — the same
snapshot-plus-changes contract the control plane's assignment watch uses, and
never a silent gap.

`retained_count` is how many retained values follow before live delivery, and
is present exactly when the watch asked for retained delivery. `0` is the
defined "no retained value" answer: joining an empty key is an answer, not a
silence indistinguishable from a slow key. Once this many changes have
arrived, the client holds the current state. A key whose newest write raced
past `resume_offset` during establishment can be absent from the retained set;
its change is already queued and arrives as the first live event, folding to
the same state.

### CacheEvent (server -> client)
```
{ "type": "cache_event", "key": "<string>", "value": "<base64|absent>",
  "offset": <u64>, "expires_at_millis": <u64|absent> }
```

One change on the watch's event stream. An absent `value` means the key was
deleted. `offset` is the change's cache-log offset — the resume anchor.
`expires_at_millis` is absolute Unix milliseconds, `0` or absent meaning never.

Offsets on a filtered watch are naturally sparse — other keys' changes consume
them — so a gap between consecutive offsets is **not** a drop signal here, the
way it is for a stream subscription. `cache_watch_lagged` is.

### CacheWatchLagged (server -> client)
```
{ "type": "cache_watch_lagged", "resume_from": <u64> }
```

The watch fell behind and its queue dropped changes; the broker delivers
everything already queued, sends this, and finishes the event stream.
`resume_from` is the offset of the first missed change: re-watching with
`from_offset = resume_from` is gapless. Loss is loud by construction, because
sparse offsets would otherwise hide it.

### CounterAdd
```
{ "type": "counter_add", "tenant_id": "<string>", "namespace": "<string>",
  "cache": "<string>", "key": "<string>", "delta": <i64>, "request_id": <u64> }
```

Sent only to a broker that advertised `FEATURE_COUNTERS`. Applies a signed
delta — negative to subtract — and is answered with `counter_value` carrying
the sum *including* this delta, so incrementing and learning where you stand
is one round trip.

A counter is scoped exactly as a cache key is: the same registered cache
scope, the same key-to-shard hash, the same owner, the same forwarding from a
non-owner. It lives beside the cache, not in it — a counter and a cache value
may share a key and are unrelated, and a cache watch does not see counter
changes.

**Delivery is at least once.** A client that retries an add after a lost
acknowledgement counts twice: deltas carry no dedupe identity. An application
that cannot tolerate a double-count keeps its own idempotency key outside the
counter. See `docs/projections.md` for the decision.

### CounterGet
```
{ "type": "counter_get", "tenant_id": "<string>", "namespace": "<string>",
  "cache": "<string>", "key": "<string>", "request_id": <u64> }
```

Answered with `counter_value`.

### CounterValue (server -> client)
```
{ "type": "counter_value", "value": <i64|absent>, "request_id": <u64> }
```

An absent `value` means the counter has never been written — a different
answer from a sum of zero, exactly as a cache miss differs from a stored
empty value.

### StreamShards
```
{ "type": "stream_shards", "tenant_id": "<string>", "namespace": "<string>",
  "stream": "<string>", "request_id": <u64> }
```

Sent only to a broker that advertised `FEATURE_STREAM_SHARDS`.

A subscription reads one shard, so a client consuming a whole stream needs to
know how many there are; nothing else on the wire says. Scoped to the client's
own tenant, and answered from the broker's routing snapshot — so it can be stale
in exactly the way any routing answer can.

### StreamShardsView (server -> client)
```
{ "type": "stream_shards_view", "shards": <u32>, "request_id": <u64>,
  "routing": "jump_hash" }
```

`0` means this broker knows nothing of that stream, which is **not** the same as
one shard. A client that rounded it up would read shard 0 and call it the
stream.

`routing` says how the stream maps routing keys to shards: `modulo` or
`jump_hash`, computed by `felix_wire::routing::shard_for_routing`. It is sent
only for a `jump_hash` stream; absent means `modulo`, so a modulo stream's
answer is byte-identical to what a broker sent before the field existed. A
client that routes keyed publishes itself must use it, or it computes a
different shard than the broker does.

### CacheShards
```
{ "type": "cache_shards", "tenant_id": "<string>", "namespace": "<string>",
  "cache": "<string>", "request_id": <u64> }
```

Sent only to a broker that advertised `FEATURE_CACHE_SHARDS`.

How many shards a cache has, so a client knows how many prefix watches to
open. It's a separate request rather than a field on `stream_shards` because
an older broker would ignore the field and answer for a stream with the same
name. Scoped to the client's tenant and answered from the routing snapshot.

### CacheShardsView (server -> client)
```
{ "type": "cache_shards_view", "shards": <u32>, "request_id": <u64> }
```

`0` means the broker doesn't know the cache. A registered cache that hasn't
been placed yet counts as one shard, as it does for `cache_watch`.

### CacheValue (server -> client)
```
{ "type": "cache_value", "key": "<string>", "value": "<base64|null>" }
```

### Ok
```
{ "type": "ok" }
```

### Error
```
{ "type": "error", "message": "<string>",
  "code": "<error code>", "retry": "<retry class>",
  "detail": { "reason": "<string>", "retry_after_ms": <u64> } }
```

A request failed. `message` is prose for people. `code`, `retry` and `detail`
are sent only to a client that offered `FEATURE_ERROR_CODES`; without them the
frame is byte-identical to the one a broker that predates codes sends. Every
field of `detail` is optional. See [Error codes](#error-codes).

`publish_error` carries the same three optional fields next to its
`request_id` and `message`, under the same negotiation.

## Semantics (v1)
- Subscribe starts at the tail unless `start` asks otherwise; a durable stream
  can be replayed from any retained offset. History read from disk joins live
  delivery with no gap and no duplicate — see
  [durable storage](durable-storage.md#resuming-a-subscription).
- Publish returns `ok` when accepted by the broker unless `ack` is `none`.
- PublishBatch returns `ok` once for the batch unless `ack` is `none`.
- A stream may carry several acked publishes before any is answered. Answers
  come back in completion order, matched by `request_id`, unless the client
  negotiated [pipelining](#pipelined-publishes), in which case they come back
  in the order the stream carried the publishes.
- CachePut returns `ok` when stored (TTL is optional).
- CacheGet returns `cache_value` with `null` when missing/expired.
- CacheDelete returns `cache_value` carrying whatever was removed, and `null`
  when the key was not there. Removing a key that does not exist is an answer,
  not an error.
- CacheWatch delivers each applied write for its key or prefix — a put with its
  value, a delete as a change with none — in the cache shard's write order,
  each carrying its log offset. Resume by offset replays `[from_offset, tail)`
  from the cache's log before live delivery, joined without a gap or a
  duplicate by the same register-before-read discipline a stream resume uses. A
  resume whose history compaction collapsed is answered with `resnapshot: true`
  and current values; a watch that falls behind is ended with
  `cache_watch_lagged` naming the offset to re-watch from. TTL expiry is not a
  change: nothing is appended when an entry lapses, so no event is delivered —
  a watcher that cares about expiry reads `expires_at_millis` off the put.
- A `retained` CacheWatch delivers current state first: each matching key's
  current value at the offset of the write that produced it, then live changes
  from `resume_offset` — so a client joins and immediately holds the state
  without waiting for the next write. `retained_count` in the confirmation
  bounds the state phase, `0` meaning the key or prefix held nothing, which is
  an answer rather than a silence. The join is gapless and unambiguous: the
  same register-before-read discipline as a resume, with duplicates detectable
  by offset.
- CounterAdd folds a signed delta into a durable running sum and answers with
  the sum including it; CounterGet reads the current one, with never-written
  distinct from zero. The sum survives restart, compaction (which collapses
  applied deltas into a checkpoint without renumbering the log), and leader
  failover — the counter log replicates beside its cache shard. At-least-once:
  a retried add double-counts, stated where the semantics are.
- GroupPoll returns `group_records`, which may be empty: nothing was available
  is an answer, not an error. Each record is claimed until the broker's
  visibility timeout lapses, after which it is handed to whoever polls next.
- GroupAck and GroupNack return `cache_ok`. An ack finishes a record; a nack
  hands it back for immediate redelivery rather than after the timeout.
- **Only the broker leading a shard serves its groups.** Any other refuses with
  `error` rather than an empty batch, because the claim and the acknowledgement
  have to reach the same in-flight state — two brokers each keeping their own
  would hand out the same records.
- Backpressure: v1 is best-effort; subscribers may miss events if they fall
  behind. With event offsets negotiated a client can *detect* that loss: a gap
  between consecutive delivered offsets is a drop unless the batch's
  `skipped_before` (bit `0x0800`, see Event batch offsets) accounts for it. A
  client that did not negotiate `0x0800` also sees a gap at every
  generation-start record, which is not a drop.

### Idempotent producers

A publish whose acknowledgement never arrived is ambiguous: the record may be
on the broker, and re-sending it would land it twice. `publish_idempotent`
removes the ambiguity. A producer takes an id from the broker
(`producer_init`), numbers its batches on each shard from zero, and sends the
number with each batch. The shard's leader knows, per producer, the next
sequence it expects and where the last 64 it appended landed:

| The batch's sequence is | The leader |
| --- | --- |
| the next expected | appends it, remembers it, answers `publish_ok` |
| one it remembers, with the same payloads | answers `publish_ok` and appends nothing — the same answer the first send got, including the `Quorum` wait on the same offsets |
| one it remembers, with different payloads | refuses with `sequence_reused` and appends nothing, for a client that offered `FEATURE_SEQUENCE_REUSED`; any other client gets `publish_ok` as for a re-send, and the batch is not written |
| past the next expected | refuses with `sequence_gap` naming the expected one; what was skipped is not here, and continuing would leave a hole the producer believes is filled |
| older than it remembers | refuses with `sequence_expired`; whether it was appended cannot be told |
| from a producer it does not know, and not zero | refuses with `unknown_producer`; there is nothing to check against, and the producer must start again under a new id |
| the rest of a batch it holds only the start of | appends the records it is missing, and answers with the whole batch's offsets; a batch whose start differs from what it holds is refused with `sequence_reused`, as above |

So a producer re-sends a batch it got no answer for under the *same* sequence,
never sends a different batch under that sequence, advances only on
`publish_ok`, and stops on any refusal but `not_leader`.

**A reused sequence is caught by a payload digest.** The leader keeps, with
each batch it remembers, a CRC-64 digest of the batch's payloads (on a durable
stream it is derived from the log, so it survives a restart and a failover;
see `docs/storage-format.md`). A batch under a remembered sequence whose
digest differs is not a re-send. A client that offered `FEATURE_SEQUENCE_REUSED`
is refused with `sequence_reused`; the Rust client offers it and surfaces the
refusal as `PublishRefused` with `PublishRefusalReason::SequenceReused`. A
client that did not offer it cannot decode the reason, so it gets what it always
got: `publish_ok`, and the batch is silently not written. That gap is why a
client must never reuse a sequence whose outcome it does not know. A batch held
without a digest, known only from a producer snapshot written before digests
were kept, is treated as matching. The Kafka listener does not check digests:
Kafka answers a duplicate sequence by its numbers alone and has no error for
"same sequence, different records".

**Only the leader takes them.** A `publish_idempotent` that arrives at a broker
that does not lead the shard is refused with `not_leader`, naming the leader
and where clients reach it, rather than forwarded: forwarded, one batch could
reach the leader from two ingress brokers with nothing to tell the second
from the first. A client sends it to the broker named.

**On a durable stream the sequences are in the log.** Each record of a
producer's batch is stored with the producer id and sequence, and replicated
with them, and a broker derives every producer's place from its own log. So a
leader promoted after a failover, the destination of a planned move, and a
leader that restarted all answer a re-send the way the leader that took it
would have, and the producer carries on. A batch whose leader stopped partway
through it is finished by the re-send rather than written twice.

A producer is known while any of its batches is in the log. One whose batches
retention has removed entirely is forgotten, as is the one whose newest batch
is oldest once a shard has more than 4096, and either answers
`unknown_producer`. After a leader change that now means the producer's history
is gone, not that the leader changed; whether the batch in flight landed cannot
be told, so the client reports it rather than starting again by itself.

**On an in-memory stream the sequences live in the leader's memory**, and a
new leader answers `unknown_producer`: such a stream loses its records with its
leader too.

The id is 64 random bits, chosen by the broker, so producers from different
brokers and across a restart cannot collide with each other's sequences.

### Pipelined publishes

A client that offers `FEATURE_PUBLISH_PIPELINE` in `auth` may be granted a
publish window, answered in `auth_ok`:

```json
{"type":"auth_ok","server_flags":2047,"server_features":65060,"publish_window":256}
```

The grant is two promises about every acked publish on that connection —
`publish` or `publish_batch` with a `request_id` and an ack, any binary frame
with `FLAG_BINARY_PUBLISH_ACKED`, and every `publish_idempotent`:

- **Answers keep request order per stream.** The broker holds an answer back
  until every publish the stream carried before it has been answered. Nothing
  else changes: each publish gets the answer it would have got, only later.
  Other responses on the stream (`cache_value`, `subscribed`, and so on) are
  not held back.
- **At most `publish_window` are unanswered per connection**, across all its
  streams. At that depth the broker stops reading the connection's publishes
  until an answer is written. A client that sends more is slowed by QUIC flow
  control, not refused; the frames wait in the transport rather than in the
  tenant's share of the publish queue, which is what keeps a pipelining client
  to its fair share.

`publish_window` is present only when the client offered the bit and the broker
grants it; a broker configured with `publish_window = 0`
(`FELIX_BROKER_PUBLISH_WINDOW=0`) neither advertises the bit nor grants a
window. A client that did not offer it, and one that predates negotiation, get
exactly the frames they always got: completion-order answers and no window. A
client reads a window without the bit as no window.

It is a feature bit rather than a frame flag because no payload changes shape:
the same frames go both ways, and only their timing and order differ.

A broker that loses an answer it owes a pipelining stream would hold every
answer behind it forever. Every publish is answered within its enqueue wait
plus its ack wait, so a broker whose oldest held answer is overdue by twice
that closes the stream instead; the client sees the stream fail and every
unanswered publish on it as failed.

**Why the order matters to an idempotent producer.** With answers in request
order, the first failure a producer reads is the earliest one, never a
consequence of it: a batch refused with `sequence_gap` because the batch before
it failed is answered after that failure, not before. So a producer can keep
several batches unanswered and, when one fails, treat it and everything behind
it as in doubt and re-send them in order under the same sequences — the leader
answers the ones it already holds from memory and appends the rest. The Rust
client keeps at most 64 in flight, since the leader remembers 64 sequences per
producer and a re-send has to find its batch remembered.

## Protocol Flows (v1)

### 1) Publish/Subscribe flow (handshake + control + events)
```mermaid
sequenceDiagram
    participant Pub as Publisher
    participant SubA as Subscriber A
    participant SubB as Subscriber B
    participant B as Broker
    participant Q as Broker queue
    Note over Pub,B: QUIC connection + stream setup
    Pub->>B: ClientHello (QUIC/TLS)
    B-->>Pub: ServerHello + OK
    Pub->>B: Open control stream (bi)
    Pub->>B: publish / publish_batch
    B->>Q: enqueue publish
    alt ack = none
        Note over Pub,B: No ok frame is sent
    else ack = per_message|per_batch
        B-->>Pub: ok
    end
    Note over SubA,B: QUIC connection + stream setup
    SubA->>B: ClientHello (QUIC/TLS)
    B-->>SubA: ServerHello + OK
    SubA->>B: Open control stream (bi)
    SubA->>B: subscribe
    B-->>SubA: ok
    B-->>SubA: Open event stream (uni)
    Note over SubB,B: QUIC connection + stream setup
    SubB->>B: ClientHello (QUIC/TLS)
    B-->>SubB: ServerHello + OK
    SubB->>B: Open control stream (bi)
    SubB->>B: subscribe
    B-->>SubB: ok
    B-->>SubB: Open event stream (uni)
    loop stream events
        Q-->>B: dequeue publish
        B-->>SubA: event
        B-->>SubB: event
    end
```

### 2) Client wants to put/get data to/from cache (handshake + request/response)
```mermaid
sequenceDiagram
    participant C as Client
    participant B as Broker
    Note over C,B: QUIC connection + stream setup
    C->>B: ClientHello (QUIC/TLS)
    B-->>C: ServerHello + OK
    C->>B: Open cache stream (bi)
    C->>B: cache_put (request_id)
    B-->>C: ok (request_id)
    C->>B: cache_get (request_id)
    B-->>C: cache_value (request_id, value|null)
```

### 3) Client watches a cache key (establish + resume + live)
```mermaid
sequenceDiagram
    participant C as Client
    participant B as Broker
    participant L as Cache log
    Note over C,B: Authenticated control stream with FEATURE_CACHE_WATCH negotiated
    C->>B: cache_watch (key or prefix, from_offset? or retained?)
    Note over B: Register watcher first — pins the live edge
    B->>L: read tail
    B-->>C: event_stream_hello (uni stream)
    B-->>C: cache_watch_started (resume_offset = tail, resnapshot?, retained_count?)
    alt from_offset still in the log
        B->>L: read [from_offset, tail)
        B-->>C: cache_event × n (replayed history, offsets ascending)
    else from_offset compacted away, or retained requested
        B-->>C: cache_event × n (current value per matching key)
    end
    Note over B,C: Live: queued changes below tail are duplicates and dropped by offset
    B-->>C: cache_event (offset ≥ resume_offset)
    opt watch falls behind
        B-->>C: cache_watch_lagged (resume_from)
        Note over C: Re-watch with from_offset = resume_from — gapless
    end
```

## Binary PublishBatch
When `flags & 0x0001 != 0`, the frame payload is a binary publish batch:

```
u16 tenant_len
u8[tenant_len] tenant_id
u16 namespace_len
u8[namespace_len] namespace
u16 stream_len
u8[stream_len] stream
u32 count
repeated count times:
  u32 payload_len
  u8[payload_len] payload
```

This is the encoding for client publishes. JSON is reached only as the
compatibility fallback described under
[Publish / PublishBatch](#publish--publishbatch-compatibility-only) — a client
does not choose it, it falls back to it.

## Forwarded publish acks

A publish for a shard the receiving broker does not own is **forwarded** to the
owner and acknowledged once the owner has written it. That is correct, and it
used to be invisible — so a client kept publishing to the same entry broker
forever while every record was decrypted, re-encrypted and decrypted again on
the way. A perf session put the cost at roughly half the throughput per core:
~250 MB/s per busy vCPU direct against ~140 forwarded (#536).

When `flags & 0x0080 != 0` (always together with `0x0010`), the ack names the
owner, appended after the fields above:

```
u16 node_id_len
u8[node_id_len] node_id
u16 addr_len      (0 when the owner's client address is not published)
u8[addr_len] addr
u64 generation
```

The bit's **presence** is the signal that forwarding happened; the payload says
where to send instead. It is a hint and not a refusal — the publish already
succeeded, so a client that ignores it is exactly as correct as before, only as
slow. That is what makes it safe to add: nothing depends on the client acting
on it.

An empty `addr` decodes as *absent*, not as an empty address. It means the
cluster has not been told where clients reach that broker — the same gap
`NotLeader` has, with the same consequence: the client learns who owns the shard
but has nowhere to route to.

`generation` is the ownership generation the answer was true for, so a client
holding a cached owner can tell a newer answer from an older one rather than
letting two brokers mid-rebalance overwrite each other.

**Compatibility:** `0x0080` is only ever set for a client that advertised it in
`Auth.client_flags`. A client that did not would reject the whole frame — an
unknown flag bit is refused rather than masked off — and the frame it rejects
acknowledges a publish that *succeeded*. An ack with no owner is byte-identical
to one from before the bit existed.

The JSON `PublishOk` carries no owner. It has nowhere to put one without
changing a message every client parses, and the JSON path is compatibility
traffic that is not worth optimising — a client on it is already paying more
than forwarding costs.

## Binary keyed PublishBatch
When `flags & 0x0040 != 0` (always together with `0x0001`), the publish batch body
is prefixed with a routing key:

```
u16 key_len
u8[key_len] key
... then the Binary PublishBatch body exactly as above
```

The key decides the shard, and therefore which broker owns the batch. Every record
in a batch shares one key: a batch is acknowledged as a unit, so splitting it across
shards would make it several batches.

An empty key is a key. It hashes to a shard like any other, and is not the same as
an unkeyed frame, which always resolves to shard 0.

With `0x0008` set as well, the correlation prefix comes first and the key prefix
follows it, so `request_id` stays readable at offset 0 whether or not a key follows.

**Compatibility:** `0x0040` was added after `0x0008`. A broker predating it matches
on `0x0001`, knows nothing of the key prefix, and would misparse `key_len` as
`tenant_len`. Clients therefore MUST NOT send `0x0040` unless the broker has
advertised it — see Capability negotiation below. A client talking to such a broker
sends a keyed publish with the JSON encoding instead.

## Binary acked PublishBatch
When `flags & 0x0008 != 0` (always together with `0x0001`), the publish batch above
is prefixed with a correlation header:

```
u64 request_id
u8  ack_mode        1 = per_message, 2 = per_batch
... then the Binary PublishBatch body exactly as above
```

The prefix comes first so a receiver can read `request_id` without parsing the rest
of the frame; that is what lets the broker answer a malformed body with an error the
client can still correlate to its pending request.

`ack_mode` has no encoding for "none": an unacknowledged publish uses the plain
`0x0001` frame with no prefix, so each mode has exactly one representation on the
wire.

## Binary idempotent PublishBatch
When `flags & 0x0100 != 0` (always together with `0x0001` and `0x0008`), the batch
belongs to an idempotent producer, and its id and sequence follow the correlation
header:

```
u64 request_id
u8  ack_mode        always 2 (per_batch)
u64 producer_id
u64 sequence
... then the key prefix if 0x0040 is set, then the Binary PublishBatch body
```

It means the same as `publish_idempotent` and is answered the same way: JSON
`publish_ok`, `publish_error` or `publish_refused` on the same stream, so a refusal
keeps its typed reason. A frame with `0x0100` but not `0x0008` is refused.

**Compatibility:** clients MUST NOT send `0x0100` unless the broker advertised it.
A client talking to an older broker sends `publish_idempotent` instead.

## Binary PublishAck
When `flags & 0x0010 != 0`, the frame payload is a publish acknowledgement:

```
u8  status          0 = ok, 1 = error
u64 request_id
u16 message_len     0 when status = ok
u8[message_len] message   UTF-8, error text
```

With `0x0200` (`BINARY_PUBLISH_ACK_CODE`), set only on a failed ack and only for a
client that offered the bit in `client_flags`, the error's code follows the
message:

```
u16 code            see the table in Error codes
u8  retry           1 retry, 2 retry_after, 3 redirect, 4 outcome_unknown, 5 fatal
```

A code number the client does not know is kept as unknown and its retry class
still applies; a retry byte it does not know is read as `fatal`.

With `0x0400` (`BINARY_PUBLISH_ACK_DETAIL`) as well, set only alongside `0x0200`
and only for a client that offered it, the error's `detail` follows the code:

```
u16 reason_len      0 when there is no reason
u8[reason_len] reason     UTF-8, e.g. "moving" for shard_unavailable
u64 retry_after_ms  0 when the broker suggests no wait
```

It is `publish_error.detail` in binary: without it a binary publisher is told
`shard_unavailable` but not whether the shard is moving, fenced or still opening,
nor how long a move suggests waiting.

This is the response to a `0x0008` publish. It carries exactly the information the
JSON `publish_ok` / `publish_error` messages do; a client that published with the
JSON encoding still receives those JSON messages instead.

**Compatibility:** `0x0008` and `0x0010` were added after the initial v1 release. A
broker predating them matches on `0x0001`, does not know about the prefix, and would
misparse `request_id` as `tenant_len`. Clients therefore MUST NOT send `0x0008`
unless the broker has advertised it — see Capability negotiation below.

## Capability negotiation

Flag bits change how a payload is parsed, so a peer must never guess which ones the
other side understands. The supported set is exchanged on the `auth` handshake,
which is already the first round trip on every control stream and therefore costs
no extra latency.

A client that implements negotiation includes its own set:

```json
{"type":"auth","tenant_id":"t1","token":"...","client_flags":25}
```

A broker that implements negotiation replies with its own:

```json
{"type":"auth_ok","server_flags":25}
```

The client MUST use the advertised value to decide which encodings it may send, and
MUST NOT assume its own set is supported.

Both directions degrade without a version check, because serde-style decoders ignore
unknown fields:

| Client | Broker | Outcome |
| --- | --- | --- |
| negotiating | negotiating | `auth_ok`; client uses any advertised bit |
| negotiating | legacy | `client_flags` ignored, plain `ok` returned; client assumes `ORIGINAL_V1_FLAGS` and falls back to the JSON encoding for acked publishes |
| legacy | negotiating | no `client_flags` offered, so the broker replies with a plain `ok` and never sends a message the client cannot parse |
| legacy | legacy | unchanged |

`ORIGINAL_V1_FLAGS` is `0x0001 | 0x0002 | 0x0004` — the bits that existed before
negotiation. It is the only safe reading of an absent advertisement, and it is
frozen: adding a bit to it would make clients assume support that older brokers
do not have.

A broker MUST only send `auth_ok` in response to an `auth` that offered
`client_flags`. A client old enough not to know the variant can then never receive
it.

## Feature negotiation

A *feature* bit says a request exists. A *flag* bit says how a payload is laid
out. They are numbered in separate spaces and MUST NOT be mixed: a feature never
appears on a frame, and offering one as a frame flag would have a client claim it
can receive a shape it has no decoder for.

Features are advertised in the same handshake, in an optional field:

```json
{"type":"auth_ok","server_flags":63,"server_features":1}
```

| Bit | Name | Meaning |
| --- | --- | --- |
| `0x0001` | `FEATURE_TOPOLOGY` | The broker answers `topology` |
| `0x0002` | `FEATURE_REDIRECT` | The peer understands `not_leader` |
| `0x0004` | `FEATURE_CACHE_DELETE` | The broker accepts `cache_delete` |
| `0x0008` | `FEATURE_CONSUMER_GROUP` | The broker serves `group_poll`, `group_ack`, `group_nack` |
| `0x0010` | `FEATURE_GROUP_DEAD_LETTERS` | The broker serves `group_dead_letters`, `group_discard`, `group_redrive` |
| `0x0020` | `FEATURE_STREAM_SHARDS` | The broker answers `stream_shards` |
| `0x0040` | `FEATURE_CACHE_WATCH` | The broker accepts `cache_watch` |
| `0x0080` | `FEATURE_CACHE_WATCH_RETAINED` | The broker serves `retained` delivery on a `cache_watch` |
| `0x0100` | `FEATURE_COUNTERS` | The broker serves `counter_add` and `counter_get` |
| `0x0200` | `FEATURE_IDEMPOTENT_PRODUCER` | The broker serves `producer_init` and `publish_idempotent`, and answers the latter's refusals as `publish_refused` |
| `0x0400` | `FEATURE_CACHE_SHARDS` | The broker answers `cache_shards` |
| `0x0800` | `FEATURE_ERROR_CODES` | The client reads `code`, `retry` and `detail` on `error` and `publish_error` |
| `0x1000` | `FEATURE_SHARD_MOVED` | The client reads `shard_moved` at the end of an event stream |
| `0x2000` | `FEATURE_UNSUPPORTED` | The peer answers an unknown request with `unsupported` (see below) |
| `0x4000` | `FEATURE_SEQUENCE_REUSED` | The client reads `publish_refused` with `sequence_reused`; see [idempotent producers](#idempotent-producers) |
| `0x8000` | `FEATURE_PUBLISH_PIPELINE` | The client pipelines acked publishes; the broker grants a `publish_window` and answers each stream's publishes in request order. See [pipelined publishes](#pipelined-publishes) |

Features are advertised in **both** directions. A client offers its own in the
`auth` it already sends:

```json
{"type":"auth","tenant_id":"t1","token":"...","client_flags":63,"client_features":3}
```

The client's set matters for exactly the same reason as the broker's: a broker
must not send a client a message type it cannot decode. `not_leader` travels
broker to client, so the broker sends it only to a client that offered
`FEATURE_REDIRECT`, and answers everyone else with an ordinary `error`.

`FEATURE_CACHE_DELETE` runs the other way, because `cache_delete` is a request:
a client sends it only to a broker that advertised the bit. Getting that
backwards is worse than a refused request — an unrecognised message type ends
the broker's control loop, so probing costs the connection.

Note which features depend on what. `FEATURE_TOPOLOGY` and `FEATURE_REDIRECT`
describe a cluster, so a standalone broker advertises neither.
`FEATURE_CACHE_DELETE` works the same on one node as on twenty, and is
advertised by both, as are `FEATURE_STREAM_SHARDS` and `FEATURE_CACHE_SHARDS` —
a standalone broker has one shard per stream and per cache and can say so. `FEATURE_CONSUMER_GROUP` and `FEATURE_GROUP_DEAD_LETTERS` depend on durable
storage rather than on clustering: without it a group's position is lost on
every restart, so a broker with none offers neither. `FEATURE_CACHE_WATCH`
depends on the cache being log-backed, for the same shape of reason: a watch's
contract — resume, duplicate detection, the lag signal — is built on log
offsets, and a broker whose cache is the in-memory fallback has none to offer.
`FEATURE_CACHE_WATCH_RETAINED` travels with it, and is a bit of its own for
the reason the dead-letter bit is not folded into the consumer-group bit: a
broker built when the watch bit meant live-and-resume only would ignore the
request's `retained` field and serve a live-only watch — silent misdelivery,
which is worse than the refused request a missing bit produces.
`FEATURE_COUNTERS` depends on durable storage, like the group features: a
counter is a fold over a log, and a sum that any restart resets is worse than
refusing to count at all.

They are two bits rather than one because a bit says which requests exist, and
widening what an existing bit promises is the one change that cannot be made
safely — a broker built when `FEATURE_CONSUMER_GROUP` meant only poll, ack and
nack would advertise it and then meet a request it has no arm for.

An absent `server_features` or `client_features` means that peer implements
none. This is not a
formality. An unrecognised message `type` is a **fatal** protocol error to the
broker's control loop — it closes the connection rather than answering — so a
client MUST NOT send a featured request speculatively to find out whether it is
supported. Silence means no.

A broker advertises a feature only when it can actually answer it. A broker with
no cluster behind it has no topology to report, and advertises `0`.

## Error codes

A client that offers `FEATURE_ERROR_CODES` in `auth` gets a typed `code` and a
`retry` class on every `error` and `publish_error` the broker sends it, and on a
failed binary ack if it also offered `BINARY_PUBLISH_ACK_CODE` (and the `detail`
there too if it offered `BINARY_PUBLISH_ACK_DETAIL`). A client that did not offer
the bit gets the same frames as before, with no new fields. The broker
advertises the bit too, so a client can tell "no code applies" from "this broker
predates codes". That includes a refused `auth`: the broker reads the offer
before answering it.

The code says what happened; the retry class says what the client may do. They
travel separately so that a code the client does not know is still actionable:
unlike an unknown frame flag, an unknown code MUST NOT fail the frame. A client
keeps it as an unknown code and follows its retry class. An unknown retry class
is read as `fatal`, the one reading that cannot duplicate a write.

| Retry class | Meaning |
| --- | --- |
| `retry` | Nothing was applied; sending the request again is safe. |
| `retry_after` | Nothing was applied; wait before sending again. `detail.retry_after_ms`, when present, says how long. |
| `redirect` | Nothing was applied; send it to the broker that owns the shard. |
| `outcome_unknown` | It may have been applied. Only an idempotent request is safe to send again. |
| `fatal` | Sending it again will fail the same way. |

The retry class in the table is the one the broker sends unless noted; a broker
may send a different class for a particular failure when it knows better, and a
client MUST act on the class it received, not on this table.

| Code | Retry class | Number | Meaning | When sent |
| --- | --- | --- | --- | --- |
| `unauthenticated` | `fatal` | 1 | The stream has not authenticated, or the credential was refused. | A request before `auth`; a token that does not verify. |
| `forbidden` | `fatal` | 2 | The credential does not grant this operation. | A missing permission, or a tenant other than the token's. Also a forward the owner refused on the client's credential. |
| `not_found` | `retry_after` | 3 | The tenant, namespace, stream or cache does not exist on this broker. | An unknown stream or cache. Retryable because a broker learns streams from the control plane, and one promoted a moment ago says "not found" for a stream it is about to serve. A publish refused for any other reason keeps that reason's own code and text: an unservable shard is `shard_unavailable`, never "stream not found". |
| `invalid_request` | `fatal` | 4 | The request can never succeed as sent. | A malformed frame or batch, unknown frame flags, a missing `request_id`, a second `auth`, a bad watch filter or shard. |
| `shard_unavailable` | `retry` | 5 | Nobody can serve the shard right now. `detail.reason` says why: `not_assigned`, `owner_unavailable`, `not_ready`, `stale`, `fenced`, `moving` or `region_not_routable`. | The shard is unassigned, its owner unreachable, still opening, or the routing view is behind; `fenced` when this broker's lease lapsed, the owner's epoch was superseded, or the shard stopped serving here between admitting a write and claiming its place in the log (nothing was written). `moving` when the shard is being moved to another broker and the move had not cut over within `FELIX_SHARD_MOVE_HOLD_MS`, or too many publishes were already waiting on moving shards; `detail.retry_after_ms` then suggests when to try again. `region_not_routable` when the leader is in a region this broker has no `FELIX_REGION_BRIDGES` bridge to, so it will not forward there; a client routing to the leader directly is not refused. An older client sees the reason as text and treats the error like any other `shard_unavailable`. |
| `not_leader` | `redirect` | 6 | Another broker owns the shard. | Only where the `not_leader` message cannot be sent: to a client without `FEATURE_REDIRECT`, or a publish this broker cannot forward. |
| `quorum_timeout` | `outcome_unknown` | 7 | The leader wrote the batch; a majority did not confirm it in time. It may survive. | A write to a `Quorum` stream or cache. |
| `leadership_lost` | `outcome_unknown` | 8 | Leadership moved after the leader wrote the batch and before a majority held it. | A write to a `Quorum` stream or cache during a move. |
| `unacknowledged` | `outcome_unknown` | 9 | The broker stopped waiting for the write's outcome. | A forwarded batch whose answer never came; a commit that outlasted the ack wait; a publish whose shard's log was reset (the broker became a follower, or the log was rebuilt) while it waited behind earlier publishes. That publish reached no reader or subscriber. |
| `overloaded` | `retry_after` | 10 | The broker is shedding load. | A full ingress queue or ack path. Sent as `outcome_unknown` when the batch was already queued before the broker ran out of room to track its ack. With `detail.reason = "publish_queue_full"` when the broker's publish queue had no room for the publish's tenant (`FELIX_BROKER_PUB_QUEUE_DEPTH`); nothing was queued, the message still reads "publish queue full", and `detail.retry_after_ms` suggests a short wait. No new code or flag: a client that predates the reason retries it like any other `overloaded`. With `detail.reason = "tenant_quota"` when the publish's tenant is over its publish quota (`FELIX_TENANT_PUBLISH_*`); nothing was queued, and `detail.retry_after_ms` says when the tenant is back under. An older client sees the reason as text and backs off as for any other overload. |
| `limit_exceeded` | `fatal` | 11 | The request exceeds a configured limit. | Too many subscriptions on one connection. |
| `draining` | `retry` | 12 | The broker is shutting down and takes no new work. | In answer to `auth` on a control stream opened while the connection drains. Connect to another broker. |
| `internal` | `outcome_unknown` | 13 | Something failed inside the broker. | Anything not covered above. Sent as `retry` for reads and failures before any write, `fatal` for configuration the request cannot change. |
| `storage` | `outcome_unknown` | 14 | The storage layer failed. | A durable write, a group's state, or a cache log that could not be read. `retry` for reads. |
| `stale_claim` | `retry` | 15 | The group has no record of handing this offset out. Nothing was applied; the record will be delivered again. | A `group_ack` or `group_nack` for an offset the log holds that was written after the group's in-flight state was last rebuilt and not yet handed out. A client that does not know the code sees an unknown code with `retry`. |

`Number` is the `u16` the binary ack carries. `0` is never sent.

A publish that reaches a broker while its shard is moving is not refused at
once. Between a move's fence and its cut-over nobody serves the shard, so the
broker holds the publish, before accepting it, until its routes show where the
shard went and then sends it there, forwarded if that is another broker. A
client sees a slower acknowledgement, not an error. Only a move that takes
longer than `FELIX_SHARD_MOVE_HOLD_MS` (2 s by default), or a burst beyond
`FELIX_SHARD_MOVE_HOLD_MAX` held publishes, is answered with
`shard_unavailable` / `moving`. A client without `FEATURE_ERROR_CODES` gets
only the `publish_error` text, which names the stream and the reason, such as
`publish to t1/ns/orders refused: shard is moving to another broker`. Cache
and counter operations are held and forwarded the same way, under the same
bounds. A consumer-group operation is held too, and once the move cuts over is
answered with `NotLeader` naming the new owner (an error with code
`not_leader` to a client without `FEATURE_REDIRECT`), because group operations
are served only by the shard's leader and never forwarded. A group poll that
is waiting for records when its shard moves away answers with no records.

`quorum_timeout`, `leadership_lost` and `unacknowledged` are what separate "the
broker refused this" from "the broker cannot say": a client resending a
non-idempotent publish after one of them may write it twice. An idempotent
producer can resend safely, which is what it is for.

A client without `FEATURE_ERROR_CODES` that the broker is draining still gets no
answer on a new stream until the connection closes, as it always did.

A forwarded write that the owner refused because its epoch was fenced reaches
the client as `shard_unavailable` with reason `not_ready`, not `fenced`: the
broker-to-broker answer does not say which. The class, `retry`, is the same.

How the Rust `ClusterClient` acts on each class, including what it does with a
peer that sends no code, is in `docs/multi-node-client.md` under "Retries".

## Not-leader redirects

A subscribe for a shard the broker does not own is answered with `not_leader`,
naming the broker that does. So is every consumer-group request — `group_poll`,
`group_ack`, `group_nack`, `group_dead_letters`, `group_discard` and
`group_redrive` — since only the shard's leader holds its groups. Each group
request has a stream of its own, so the answer needs no `request_id`: it answers
the one request on that stream.

```json
{"type":"not_leader","node_id":"broker-b","addr":"10.0.0.5:5000","generation":7}
```

`addr` is the owner's **client-facing** listener, and is omitted when the
cluster has not been told one. It is given for a draining owner too: a broker
being drained is left out of `topology`, which lists where new clients should
connect, but it still leads the shards it has not handed off, and a redirect to
one of those has to say where it is. The same holds for the owner hint on a
forwarded publish's ack — a client is then given the owner's name alone,
which is still usable if it knows that broker from `topology`. Dialling the
broker-internal listener instead would be refused, so no address is better than
the wrong one.

`generation` is the assignment epoch the answer describes. A client holding a
newer one has already moved on and should ignore the redirect.

Only a client that offered `FEATURE_REDIRECT` receives `not_leader`. One that
did not gets the `error` it always did, naming the owner in prose.

A client following a redirect MUST bound its hops. A cluster mid-rebalance can
name an owner that names another, and two brokers that disagree would otherwise
bounce a client between them indefinitely. The Rust `ClusterClient` caps this at
three hops and refuses to visit the same broker twice within one attempt. It
follows redirects for subscribes, cache watches and every group request; the
Python and TypeScript clients' group calls go through it. The single-broker Rust
`Client` returns the redirect as `NotLeaderError` and does not follow it.

**Publish is not redirected — it is forwarded.** The two paths made opposite
choices deliberately: `docs/subscribe-routing.md` records the measurements
behind redirecting subscribes, and `docs/internal-protocol.md` the forwarding
of publishes. A client should not expect `not_leader` in answer to a publish.

## Shard moves

A broker that stops serving a shard, because a rebalance or a drain moved it,
ends every subscription and cache watch it was serving on that shard. It waits
(briefly) for the writes already inside the shard's write fence to land and fan
out, so each reader first receives everything this broker committed. To a client
that offered `FEATURE_SHARD_MOVED` it then sends `shard_moved` and finishes the
stream.

- `resume_from` is the first offset this broker did not offer the reader. For a
  stream subscription every record below it was sent to the subscriber or dropped
  by the subscriber's own queue, and none at or above it was. It is the stream's
  position, not the subscriber's: a queue that dropped records does not move it.
  Absent for an in-memory stream, whose sequence means nothing on another broker.
  For a cache watch it is the shard log's tail, and absent if writes were still
  in flight when the watch ended; resume after the last offset seen then.
- `node_id` and `addr` name the broker taking the shard, when this one knows:
  the successor while the move is in progress, the new leader after. They are a
  hint. The shard may not be served there yet (the frame goes out at the fence,
  before the cut-over), or may have moved again, and a subscribe there is then
  answered like any other: `not_leader`, or `shard_unavailable` to retry.
- `generation` is the assignment generation that moved the shard.

A client resumes a durable subscription at `max(last delivered offset + 1,
resume_from)`. The `max` matters in both directions. A record fanned out just
before the move can still reach the subscriber, so the last offset it saw may be
past `resume_from`; and records its queue dropped stay dropped, exactly as they
would have without the move. Resumed that way, nothing is repeated and nothing
is skipped that the subscriber would otherwise have received.

The Rust `ClusterClient` does this on its own: a subscription from
`ClusterClient::subscribe` and a cache watch from `ClusterClient::watch_cache`
follow their shard, and sharded subscriptions and watches report
`ShardEvent::ShardMoved` and `ShardedCacheWatchItem::ShardMoved` and resume the
shard. A cache watch resumes at `max(offset after the last change it
delivered, resume_from)`. `Client` surfaces the frame as
`Subscription::shard_moved` and `CacheWatchItem::ShardMoved` and leaves the
resume to the caller.

## Topology

`topology` asks a broker which brokers a client may connect to. It is sent on an
authenticated control stream, and only to a broker that advertised
`FEATURE_TOPOLOGY`.

```json
{"type":"topology"}
```

```json
{"type":"topology_view","brokers":[
  {"node_id":"broker-a","addr":"10.0.0.4:5000"},
  {"node_id":"broker-b","addr":"10.0.0.5:5000"}
]}
```

`addr` is the broker's **client-facing** listener, which is a different listener
from the one brokers forward to each other on. A broker is listed only when the
cluster considers it able to serve and it has advertised where clients reach it;
one that has not is omitted rather than reported at an address that would refuse
the connection.

An empty list is a valid answer, not an error: it means the cluster has named no
client-reachable broker. A client MUST treat the answer as additive and keep the
endpoints it was configured with, so that a wrong or stale answer can never leave
it with fewer ways in than it started with.

The response carries no placement, capacity, or liveness detail. Those are the
cluster's business, and a tenant's client has no standing to read them.

## Shared Binary EventBatch
When `flags & 0x0004 != 0`, the event-stream frame payload is:

```
u32 count
repeated count times:
  u32 payload_len
  u8[payload_len] payload
```

The subscription is identified by the preceding `EventStreamHello`, so event
batches carry no per-subscriber identifier and the broker can share one encoded
frame across subscribers. The legacy `0x0002` format remains decodable.

## Event batch offsets

When `flags & 0x0020 != 0`, a `u64 base_offset` precedes the `u32 count` — after
the `u64 subscription_id` on the `0x0002` form, and at the very start of the
payload on the shared `0x0004` form:

```
u64 base_offset          # offset of the first payload
u32 count
repeated count times:
  u32 payload_len
  u8[payload_len] payload
```

A batch's offsets are contiguous, so one `u64` per *batch* is enough: payload
`i` sits at `base_offset + i`. That is what keeps offsets off the per-event cost
model — 8 bytes per batch, not per event.

Offsets belong to the **stream**, not to the subscriber, so the shared
encode-once frame still serves every subscriber that negotiated the bit. A
subscriber that did not negotiate it receives the frame without the field, so
the broker encodes at most one extra variant per batch regardless of how many
subscribers there are.

Two uses:

- **Checkpointing.** A client records the offset it last handled and resumes at
  that offset plus one after a reconnect.
- **Detecting loss.** Subscriber queues drop under `DropNew`, so consecutive
  delivered batches can have a gap. Without offsets that loss is invisible; with
  them it is a discontinuity the client can see and act on. Not every offset
  holds an event, though (below), so the number of events dropped between an
  event at `previous` and the next at `offset` is
  `offset - previous - 1 - skipped_before`.

### Offsets that hold no event

A durable stream's log also holds records that are not events. Once the fleet
has finalized `generation_start`, a leader appends a **generation-start**
record whenever it starts serving a shard at a new generation, so its quorum
mark can cover the records it inherited: after a promotion, at either end of a
move, and when a cancelled move hands the shard back. Each step of a move is a
new generation, so a move can leave several in a row. A record takes the next
offset like any other and is never delivered — not to a subscription, a
consumer group, or a Kafka fetch. A subscriber therefore sees `N-1` and then
`N+1` across a generation start at `N`.

When `flags & 0x0800 != 0` (only ever alongside `0x0020`), a `u64 skipped_before`
follows `base_offset`:

```
u64 base_offset          # offset of the first payload
u64 skipped_before       # offsets just below base_offset that hold no event
u32 count
...
```

The broker sets it only on the batch whose first event follows such offsets, so
every other batch is byte-identical to the `0x0020` form. Like the offsets it is
a property of the stream, so it rides the shared frame as one more rare
variant. A client opts in by offering `0x0800` in `client_flags`; the broker
sends the bit only to a client that offered it. A frame with `0x0800` and not
`0x0020` is rejected.

It counts only the offsets *immediately* before the batch. If the batch carrying
it is itself dropped, the next gap includes those offsets and overstates the
drop by them — a drop is still reported, only its size is off.

A client that did not offer `0x0800` gets the frames it always got, and reads
the gap at generation starts as a drop. That is a false drop signal, once per
leader change or move, and only after `generation_start` is finalized.

## ALPN

Client listeners use the ALPN id `felix/1` (`felix_wire::CLIENT_ALPN`). A
client offering it gets it; a client offering only other ids (for example a
broker's `felix-internal/1`) fails the handshake. A client offering no ALPN is
still accepted, because QUIC's strict ALPN rule would otherwise lock out every
client built before the id existed: the broker reads the ALPN extension from
the ClientHello and starts the handshake on a config with or without ALPN to
match. `FELIX_TLS_REQUIRE_ALPN=true` removes that compatibility path. A client
should offer `felix/1` only to brokers that have it; an older broker lists no
ALPN and refuses any client that offers one.

For that reason the shipped clients offer `felix/1` only when asked:
`felix_client::quic_client_config(roots, true)` in Rust, `offer_alpn=True` in
Python, and `offerAlpn` in TypeScript. Turn it on in every client before
setting `FELIX_TLS_REQUIRE_ALPN` on the brokers.

## Unknown requests and the extension area

A broker that advertises `FEATURE_UNSUPPORTED` (0x2000) answers a request whose
`type` it does not know with
`{"type":"unsupported","request_type":...,"request_id":...}` and keeps serving
the stream, provided the client offered the same bit. A client that did not
gets the old behaviour: the stream is closed. `{"type":"extension","name":...,
"request_id":...,"body":...}` is a reserved area for requests named by string
rather than by a new variant; today's broker implements none and answers every
one with `unsupported` (with `extension` set to the name). Decoders map an
unknown `type` to `Message::Unknown` rather than failing, so a new request no
longer needs its own feature bit before a client can try it.

`stream_shards_view` may carry `routing` (`modulo` or `jump_hash`); absent means
`modulo`. The broker sends it for a stream created with `jump_hash` routing and
omits it otherwise.

## Future Compatibility
- Undefined `flags` bits are reserved. Receivers MUST reject frames carrying an
  unrecognised bit instead of ignoring it: flag bits select the payload layout, so
  masking an unknown bit off means confidently misparsing the body rather than
  failing. `0x0008` is the cautionary case — see Binary acked PublishBatch.
- Future message types must be version-gated.

## Test Vectors
Client implementations MUST validate against shared vectors in:
`crates/protocol/felix-wire/tests/vectors/`

## Conformance
All clients SHOULD pass the shared conformance suite (felix-conformance).
