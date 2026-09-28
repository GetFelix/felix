# Broker-internal forwarding protocol

The protocol brokers speak to each other. Separate from the client protocol in
`docs/protocol.md`, and deliberately not a superset of it.

## Why it is a separate protocol

A client frame and an internal frame must never be mistaken for one another. A
client that could send `ForwardPublish` could write to a shard on a broker that
never checked ownership; a broker that accepted client frames on its internal
listener would treat a peer as an authenticated publisher.

Three things keep them apart, and only the first is a wire concern:

1. **A distinct magic.** `FLXI` rather than `FLX1`, so a frame from one side
   fails to decode on the other rather than parsing into something plausible.
2. **A distinct listener.** Internal traffic arrives on the broker-internal QUIC
   role, never the client one. The two bind different ports, and startup refuses
   a configuration where they share one. Both internal endpoints negotiate the
   `felix-internal/1` ALPN and the client-facing role negotiates none, so a
   client pointed at the internal port has no protocol in common with it and TLS
   refuses the handshake — before a frame is read, and before any broker state
   is touched.
3. **A distinct credential.** Peer authentication is mTLS between brokers, with
   the certificate's name checked against the node id in both directions, when
   `FELIX_INTERNAL_TLS_CERT`, `_KEY` and `_CA` are set.

The magic alone is not security — it is what makes a misdirected connection fail
loudly instead of quietly.

## Framing

```
+--------+---------+------+--------+---------+
| magic  | version | kind | length | body    |
| u32    | u16     | u16  | u32    | length  |
+--------+---------+------+--------+---------+
```

Big-endian throughout, matching the client protocol.

`kind` is a message discriminant, not a flag set. The client protocol uses flag
*bits* because they modify one payload layout; here each kind **is** a layout, and
an enum makes "unknown kind" a single unambiguous check.

**An unknown kind is refused, never interpreted.** Same reasoning as the client
protocol's unknown flag bits: the kind selects how to read the body, so ignoring
one means confidently misparsing it.

Refused, though, not fatal. The receiver reads the body — the frozen header says
how long it is — and answers `UnsupportedKind` against the correlation id, then
carries on with the next frame. That distinction is what makes *adding a kind*
an additive change: these streams are long-lived and multiplex every in-flight
request to a peer, so dropping one on an unrecognised frame would turn "the peer
is newer than me" into "every request in flight to that peer failed", and make
every protocol addition a cutover.

It rests on one invariant: **every body begins with its correlation id, and
nothing may be added before it.** Without that the refusal could not be matched
to the request that caused it, and closing the connection would be the only
option left. `every_body_begins_with_its_correlation_id` holds every kind to it.

A frame that is not *ours* is still fatal — a wrong magic, or a version this
build does not speak. There is nothing to step over, because the bytes are not
laid out the way the reader assumes, so its length field means nothing.

### Handshake

The first message on a connection is `Hello`, answered by `HelloOk`. Both carry
a node id.

It exists for *when* a mismatch is found, not for what is found. Every frame
already carries the version, so a mismatched peer would be rejected on its first
request either way — but that request would be a real forwarded publish, which
then has to be failed and retried. Doing it at connect makes the connection
unusable before anything is riding on it.

The node ids make one further check possible: the caller compares `HelloOk`
against the node id it dialled. An address the catalog has since reassigned
answers with a different id, which is a connection to the wrong broker whether
or not it would have served the request.

### Capabilities

A build that has something to offer beyond the base protocol says so in the
handshake. The caller sends `HelloCapable` (kind 27), which is `Hello` followed
by a `u64` of capability bits, and a responder that knows the kind answers
`HelloCapableOk` (kind 28), `HelloOk` followed by its own bits. A responder that
predates them refuses kind 27 as `UnsupportedKind`, and the caller greets it
again with the plain `Hello`. A peer greeted either way with the plain kinds has
no capabilities, and the plain kinds stay byte for byte what they were.

Each bit says a peer answers some request. None changes how a body is read, so a
bit this build does not know is carried and never asked about, rather than
refused as an unknown client flag is. The pool refuses to send a request whose
bit the peer did not offer (`PeerError::Unsupported`), and nothing is written.
Both ends note what the other offered, the dialler from the answer and the
listener from the offer, because a leader deciding what its replicas can do may
never have dialled the one that shipped to it.

| Bit | Name | The peer |
| --- | --- | --- |
| `1 << 0` | `FENCE` | answers `Fence`, and refuses every older leader of the shard once it has |
| `1 << 1` | `TAIL_FETCH` | answers `ReplicateFetch` from the leader that fenced it |

`FELIX_INTERNAL_FENCE=false` turns both bits off: the broker offers nothing
and refuses `Fence` and `ReplicateFetch` as unknown kinds, as an older build
would.

### Versioning

`version` is this protocol's own, independent of the client protocol's. A peer
speaking a version this broker does not know gets `ProtocolVersion` and the
connection closes; there is no partial understanding.

Additive evolution happens by adding kinds, not by bumping the version. A new
kind on an old peer is an unknown kind, which is already a typed error rather
than a misparse. The version exists for the case additive change cannot cover —
a change to the header or to an existing body layout.

## Correlation

Every request carries a `correlation_id`, unique per connection and chosen by the
requester. Every response echoes it.

**Every request has exactly one terminal response.** `ForwardPublish` is answered
by exactly one of `ForwardPublishOk`, `ForwardPublishError`, or `NotLeader`;
`ForwardCacheOp` by exactly one of `ForwardCacheOk`, `ForwardCacheError`, or
`NotLeader`. A requester that never receives one, because the connection
dropped, treats the request as failed — nothing is silently abandoned as
pending.

The two paths share `NotLeader`, which carries a routing answer rather than an
error, and share nothing else. A refusal is answered in the shape of the request
that caused it: a requester matches on the message kind to decide what happened,
so a cache operation refused with a publish's error type would read as a
protocol violation rather than as a refusal.

Correlation ids are per connection, so two connections may reuse a value.
Responses are matched within the connection they arrive on and nowhere else.

## Flows

### Forwarded publish

```mermaid
sequenceDiagram
    participant A as Broker A (ingress)
    participant B as Broker B (owner)

    A->>A: resolve shard -> Remote(B, generation)
    A->>B: AuthorizedForwardPublish(correlation, shard, generation, payloads, credential)
    B->>B: check own ownership at that generation
    B->>B: verify the credential allows stream.publish here
    alt B owns the shard at that generation and the client may publish
        B->>B: append and commit locally
        B-->>A: ForwardPublishOk(correlation, first_offset, last_offset)
    else B has moved on
        B-->>A: NotLeader(correlation, owner, its generation)
    else the credential is missing or does not allow it
        B-->>A: ForwardPublishError(correlation, Unauthorized)
    else B cannot serve
        B-->>A: ForwardPublishError(correlation, code)
    end
```

The `generation` field is what makes this safe. A forwarding broker names the
assignment generation it resolved against; the owner compares it with its own.

- **Equal** — both agree, and the publish proceeds.
- **Requester behind** — the shard moved. `NotLeader` carries the new owner, so
  the requester can retry against it rather than guess.
- **Requester ahead** — the *owner* is behind, and answers `StaleRoute`. It must
  not accept the write: it would be writing a shard it may no longer hold.

That asymmetry is the point. A generation mismatch in either direction is an
explicit typed answer, and **never a successful ownership claim**.

### Authorization across a forward

Ownership says the owner *may* write the shard. It says nothing about whether
the client behind the forward may, and the owner cannot tell an honest
forwarder from anything else that can reach its port. So a forward carries the
client's own bearer token, and the owner verifies it exactly as a direct
request is verified: against the tenant's keys, for `stream.publish` on that
stream (or `cache.read` / `cache.write` for a cache operation). A credential
that is missing, does not verify, or does not allow the action is refused
`Unauthorized` before anything is written. The owner's decision therefore
depends on the client's authority, not on the forwarder's honesty — which is
the property peer authentication alone cannot give.

The credential rides two kinds of its own, `AuthorizedForwardPublish` (22) and
`AuthorizedForwardCacheOp` (23): the legacy layouts are frozen, so the field is
appended on a new kind rather than added to the old one. A forwarder holding a
credential always sends the authorized kind — the credential decides the kind,
not a flag — and the legacy kinds, though still decodable, are refused by an
owner on this build for the same reason a missing credential is. An owner from
*before* these kinds answers them `UnsupportedKind`; the forwarder then sends
the legacy kind once, which is all that owner can read and which it checks no
differently, so a rolling upgrade keeps forwarding in that direction. See
[upgrades](../docs-site/src/content/docs/deployment/upgrades.md) for the
other direction.

An authorized frame relabelled as the legacy kind has trailing bytes and is
refused; a legacy frame relabelled as authorized has no credential to read and
is refused. A peer cannot pick the weaker check by rewriting the kind.

**The `shard` a forward names must be the shard the route was resolved for.**
The owner appends to the shard the message names, so the routing decision and
the batch have to agree: a record whose key resolved to shard 3, forwarded with
shard 0, is written to the wrong log. Nothing downstream detects it — the owner
legitimately owns shard 0 and the write succeeds — so a whole multi-shard
stream's keys can silently collapse onto one shard while the router dispatched
them correctly all along.

### Replicating a cache

A cache shard is a log, so it is replicated by the same exchange as a stream's:
`ReplicateCacheRecords` carries the same body as `ReplicateRecords`, and the
follower runs the same tail, gap, and divergence checks against it.

**The kind is in the message kind, not in the shard reference.** A cache and a
stream may share a name, and a follower that guessed wrong would append one's
records into the other's log. A separate kind rather than a new field, because
this protocol evolves by adding kinds: widening `ShardRef` is a change to an
existing body layout, which needs a version bump, and a version bump means an
upgraded broker cannot talk to one that has not restarted yet.

An old peer answers an unknown kind with `Malformed`, which is not retryable, so
a leader shipping to a peer that predates this stops and says so rather than
silently writing a cache's records somewhere they do not belong.

One property the cache brings that a stream does not: **compaction must not
renumber the log.** Records are shipped at their offsets, so a leader that
restarted numbering on compaction would make its offset 0 a different record
from every follower's. Cache compaction appends the live set at the tail for
that reason.

### Replicating group state

A stream shard carries two more logs beside its records: the positions its
consumer groups have reached, and the offsets those groups gave up on. Each is
shipped by the same exchange with a kind of its own —
`ReplicateGroupRecords`/`ReplicateGroupBootstrap` (kinds 16 and 17) for the
cursors, `ReplicateDeadLetterRecords`/`ReplicateDeadLetterBootstrap` (kinds 18
and 19) for the dead letters — same bodies, for the reason the cache pair
exists: the kind is the only thing stopping a follower from appending the
offsets a group abandoned into the cursor log that says where it resumes.

Both ride the shard's own replica set at the shard's generation, and neither
gates the records: no publish waits on group state, and group state lagging
never holds up the log it describes. What this buys is a promotion that keeps
the whole group, not half of it — the promoted leader resumes each group where
it had reached *and* can list and redrive what it had given up on.

A cache shard has the same shape of companion:
`ReplicateCounterRecords`/`ReplicateCounterBootstrap` (kinds 20 and 21) ship
its counter log on the cache's replica set, so a promoted replica folds the
true sum rather than restarting it. And the forwarded cache operation grew two
op kinds beside put/get/delete — `CounterAdd` (4) and `CounterGet` (5) — with
the delta and the sum riding the envelope's existing value bytes as eight
big-endian bytes, so the body layout is untouched and an old peer refuses the
op rather than misparsing it.

### Forwarded cache operation

A cache key hashes to a shard, and that shard has one owner. A broker that
receives an operation for a key it does not own hands it over rather than
serving it, which is what stops two brokers holding different values for one
key.

```mermaid
sequenceDiagram
    participant A as Broker A (ingress)
    participant B as Broker B (owner)

    A->>A: hash the key -> shard -> Remote(B, generation)
    A->>B: AuthorizedForwardCacheOp(correlation, shard, generation, op, key, value, ttl, credential)
    B->>B: check own ownership at that generation
    B->>B: verify the credential allows the op (cache.read or cache.write)
    alt B owns the shard at that generation and the client may
        B->>B: apply to its cache log
        B-->>A: ForwardCacheOk(correlation, value?)
    else B has moved on
        B-->>A: NotLeader(correlation, owner, its generation)
    else the credential is missing or does not allow it
        B-->>A: ForwardCacheError(correlation, Unauthorized)
    else B cannot serve
        B-->>A: ForwardCacheError(correlation, code)
    end
```

The same generation rules as a forwarded publish, and the same reason: a
mismatch in either direction is a typed answer, never a successful ownership
claim. The same credential check too — `Get` and `CounterGet` need
`cache.read`, everything else `cache.write` — for the reason given under
[Authorization across a forward](#authorization-across-a-forward).

`ForwardCacheOk` carries the value a `Get` found or a `Delete` removed, and
nothing for a `Put`. The value is length-prefixed **behind a presence byte**, so
a stored empty value stays distinguishable from a miss — the two mean different
things, and one encoding for both would make a cached empty value read as
absent forever.

A `Get` and a `Delete` are safe to re-send after an indeterminate answer,
because both land on the same state twice. A `Put` is not: re-sending one with a
TTL restarts its clock. So a lost answer to a write is reported as
indeterminate rather than retried, and the caller decides.

### Handshake

```mermaid
sequenceDiagram
    participant A as Broker A (caller)
    participant B as Broker B

    A->>B: Hello(correlation, node_id = A)
    alt B speaks this version
        B-->>A: HelloOk(correlation, node_id = B)
        Note over A: check the id matches the peer dialled
    else version B does not know
        B--xA: connection closed
    end
```

With capabilities, the same exchange opens with the kind that carries them:

```mermaid
sequenceDiagram
    participant A as Broker A (caller)
    participant B as Broker B

    A->>B: HelloCapable(correlation, node_id = A, bits)
    alt B knows the kind
        B-->>A: HelloCapableOk(correlation, node_id = B, bits)
    else B predates it
        B-->>A: UnsupportedKind(correlation)
        A->>B: Hello(correlation', node_id = A)
        B-->>A: HelloOk(correlation', node_id = B)
        Note over A: B has no capabilities
    end
```

### Subscribe

A subscription for a shard this broker does not own is **redirected**, not
proxied. See [subscribe routing](subscribe-routing.md) for the decision and the
measurements behind it.

The primitive is `NotLeader`, which carries the owning node's id, its advertised
address, and its generation. It is the answer to a misrouted subscribe, and it
is also what ends a live subscription whose shard has moved.

No relay kinds are defined, because nothing relays. Should proxying ever be
added they are additive: an unknown kind is already a typed error rather than a
misparse, so it needs no version bump.

### Replication

A shard leader ships records it has committed to the followers in the shard's
replica set. `ReplicateRecords` carries the leader's epoch, the offset its first
payload belongs at, a checksum over the batch, and the payloads.

The offsets are the **leader's**. A follower stores a record at the leader's
offset or not at all, which is what makes the two logs comparable by offset —
the acknowledged mark, the catch-up range, and the caught-up test that gates
promotion all rest on it.

The follower cannot tell its log where to put a record; the log appends at its
own tail. So position is *verified* rather than commanded: the batch must begin
exactly at the follower's tail.

| The batch | Answer | What the leader does |
| --- | --- | --- |
| begins at the tail | `ReplicateOk` | send the next batch from `durable_offset` |
| begins past the tail | `LogGap`, with `expected_offset` | resume from `expected_offset` |
| comes from a newer generation and begins past records the follower wrote under an older one, above the commit offset | `LogGap`, with the first such record | resume from `expected_offset`, so those records are compared |
| lies entirely below the tail | `ReplicateOk` | nothing; it was already stored |
| straddles the tail | `ReplicateOk` | nothing; the new suffix was stored |
| disagrees on stored bytes | `LogConflict` | **stop** |
| fails its checksum | `Malformed` | resend the same records |
| names an older epoch | `FencedEpoch` | stop; this broker is no longer the leader |
| names an epoch older than one this follower has accepted, even across a restart | `FencedEpoch` | stop |
| names a newer epoch than the follower knows | `StaleRoute` | retry shortly |

The middle two rows are what make a resend safe. Replication has to be able to
resend a batch whose acknowledgement was lost, and resending must not duplicate
a record: an overlap is resolved by position, and the overlapping bytes are
compared rather than assumed.

`ReplicateOk.durable_offset` is one past the last record the follower holds **on
disk**. It is both the acknowledgement and the offset to send next. It never
reports buffered data: a follower acknowledging before its own fsync would let
the leader believe a record had survived a failure it would not have survived,
and under `Quorum` that belief is the guarantee.

The batch checksum covers each payload's length as well as its bytes. Without
the length, a batch resplit in transit hashes the same as the original — and a
resplit batch is a different set of records, which is exactly the divergence the
checksum is there to catch. `felix_wire::internal::batch_checksum` is the single
definition, so the two sides cannot compute it differently.

**Producer marks** ride with the records they belong to. A record an
idempotent producer wrote carries its producer, sequence and batch length (see
`docs/storage-format.md`, "Producer marks"), and a follower has to store them
exactly as the leader did: they are how a promoted replica, or a move's
destination, answers the producer's re-send. A batch with any marked record
travels as `ReplicateMarkedRecords` (kind 25): the `ReplicateRecords` body
followed by one mark per payload — a byte, `0` none, `1` opens a batch, `2`
continues one, the opening byte followed by `producer_id u64`, `sequence u64`
and `len u32`. The batch checksum covers the marks after the payloads, only
when there are any, so an unmarked batch is byte for byte what it always was.
A follower that predates the kind refuses it, and the leader stops shipping to
it and says so, rather than the follower storing records without their marks.
Two records with the same bytes and different marks are a `LogConflict`.

**The commit offset** rides with the records too. Under `Quorum` the leader
sends its quorum mark, one past the last record a majority holds and the
control plane has been told about, as `ReplicateRecords.commit_offset`. A batch
that carries one travels as `ReplicateCommittedRecords` (kind 26): the
`ReplicateRecords` body, one mark per payload as in kind 25, then the log it
belongs to (`ReplicaLog`, `u8`) and `commit_offset u64`. A batch without one
travels as its log's own kind, byte for byte as before. A follower that predates
kind 26 answers `UnsupportedKind`; the leader resends that batch without the
offset and ships that follower the old kinds from then on. The follower keeps
the lower of the offset and the end of what the batch left level with the
leader, and will not truncate or rebuild below it.

`LogConflict` does not converge by *retrying* — the same batch meets the same
bytes. It can be repaired, and the follower does it without an exchange: a
conflict from a **newer** generation than the one this follower last accepted,
at or after where that older generation began, is a suffix a dead leader left
behind and no majority adopted. The follower truncates it and replication
resumes, unless the cut would reach below its commit offset: then a majority
acknowledged part of the suffix, and the conflict stands.

Both conditions matter. A leader disagreeing with *itself* is an inconsistency
rather than a predecessor's leftovers, and a divergence reaching further back
than the older generation's start is not that generation's to discard. Anything
else halts, as before, and is surfaced. See `docs/replication-design.md`,
"Divergence and truncation"; the invariant the storage layer actually needs is
narrower than "never rewritten", and stated there.

### Bootstrapping a follower

A follower can be positioned below everything the leader still holds — a new
replica of a stream with retention, or one that fell far enough behind. Shipping
cannot bridge that: the records in between are gone from the leader too.

`ReplicateBootstrap` carries the offset of the oldest record the leader still
has. It is the one fact a follower cannot work out for itself, and the fact it
needs before it may place a log that begins anywhere other than zero.

| The follower | Answer |
| --- | --- |
| holds no log for the shard | places it at `base_offset`, `ReplicateOk` |
| already holds a log starting there | `ReplicateOk`; the offer is idempotent |
| holds records starting elsewhere | `LogConflict` |

The last row is the point. A log placed over existing records would have a hole
between what the follower held and what it was given, and a log with a hole is
one nothing downstream can detect — from the follower's own view its offsets are
still contiguous. Discarding those records is not something a bootstrap may do,
so the leader halts that follower instead, and rebuilds it under its policy.

### Rebuilding a follower

`ReplicateRebuild` (kind 24) is the one message that discards records. It names
the shard, which of the shard's logs (`ReplicaLog`: stream, cache, group
cursors, dead letters, or counters), and the leader's oldest surviving offset.
A follower of that shard at that generation discards its copy of that log,
generation history included, and answers `ReplicateOk` with the offset it was
given, where its new copy begins. Shipping then resumes from there as if the
follower had been bootstrapped.

| The follower | Answer |
| --- | --- |
| follows the shard at that generation | discards the log, `ReplicateOk` at `base_offset` |
| holds records below its commit offset at or above `base_offset` | `LogConflict`; it stays halted |
| knows a newer generation | `FencedEpoch` |
| is outside the replica set | `Unauthorized` |
| predates the kind | `UnsupportedKind`; it stays halted |

The fence matters more here than anywhere: a superseded leader telling a
follower to discard its copy is the most damage a stale leader could do. The
leader sends it only for a halt discarding would resolve (`diverged`,
`needs_bootstrap`), never when it is itself fenced, and only within the policy
in `docs/replication-design.md`, "Rebuilding a halted follower".

The same fence applies as to storing records: a superseded leader cannot place a
log, and a broker outside the replica set cannot be given a shard. Placing a log
and filling it are the same authority question, and a fence applied to one and
not the other is a fence with a way round it.

### Fencing a promotion

`Fence` (kind 29) is sent by a newly promoted leader to a replica that offered
the `FENCE` capability, and never to one that did not. It names the shard, the
leader's generation, and the shard's own log (`ReplicaLog` stream or cache).
The replica persists the generation, fsynced, before it answers, and from then
on refuses any leader older than it: records, bootstraps, rebuilds, and a
batch that would otherwise have been told where to resume all get
`FencedEpoch`. The shard's cursor, dead-letter and counter logs check the
shard's own log as well as theirs, so the fence covers them without a request
each.

The answer, `FenceOk` (kind 30), says where the replica's copy stands: one past
its last record, its commit offset, and the generation its last record was
written at. See `docs/replication-design.md`, "Fencing a promotion", for what
the leader does with it.

| The replica | Answer |
| --- | --- |
| has accepted this generation or an older one, and its routing view is not past it | persists it, `FenceOk` |
| is asked again at the generation it took | `FenceOk` again |
| has accepted a newer generation, or its routing view has one | `FencedEpoch` |
| is outside the replica set | `Unauthorized` |
| names a log other than the shard's own | `Malformed` |
| predates the kind, or runs with `FELIX_INTERNAL_FENCE=false` | `UnsupportedKind`; the leader should never have sent it |

The fence comes with a promotion, usually before the replica's routing view
has the new generation, so a replica that is behind takes it rather than
answering `StaleRoute`.

`ReplicateFetch` (kind 31) is how the leader takes a tail a fenced replica
holds past its own. It names the shard, its own log, an offset and a byte
budget, and is answered with the records as a leader ships them
(`ReplicateRecords`, or `ReplicateMarkedRecords` when any record carries a
producer mark), at most 4 MiB and none past the replica's end. Only a leader
at exactly the generation the replica last accepted is answered: an older one
gets `FencedEpoch`, and one that has not fenced it yet `StaleRoute`.

```mermaid
sequenceDiagram
    participant B as Broker B (promoted at G+1)
    participant C as Broker C (replica)
    participant A as Broker A (deposed at G)

    B->>C: Fence(shard, G+1)
    Note over C: persist G+1, fsync
    C-->>B: FenceOk(log_end, commit_offset, last_generation)
    Note over B: majority fenced, B counted
    opt C's log is ahead of B's
        B->>C: ReplicateFetch(shard, G+1, from)
        C-->>B: ReplicateRecords(...)
    end
    Note over B: opens for writes
    A->>C: ReplicateRecords(shard, G, ...)
    C-->>A: ReplicateError(FencedEpoch)
```

## Errors

Typed, because they need different responses:

| Code | Meaning | What the requester does |
| --- | --- | --- |
| `NotLeader` | the shard is owned elsewhere | retry against the named owner |
| version mismatch | the peer speaks a version this broker does not know | the connection closes; there is no frame to answer with, because a reply would carry the version the peer just rejected |
| `StaleRoute` | the *responder* is behind the requester's generation | retry shortly; the owner is catching up |
| `Unavailable` | the owner cannot serve right now, e.g. still opening | retry with backoff |
| `Unauthorized` | the peer is not permitted, or the credential a forward carries is missing or does not allow the write | do not retry |
| `Overload` | the owner is shedding load | retry with backoff |
| `ProtocolVersion` | version not understood | do not retry; close |
| `Malformed` | the body did not decode | do not retry; close |
| `StorageFailed` | the responder tried and its own disk failed | do not retry; nothing is wrong with the request |
| `LogGap` | a replication batch starts past the follower's tail, or past its uncompared older-generation records | resume from `expected_offset` |
| `LogConflict` | a replication batch disagrees with stored bytes | do not retry; the logs have diverged |
| `FencedEpoch` | the sender named an epoch older than the responder's | do not retry; it is no longer the leader |
| `UnsupportedKind` | the responder predates the kind that was sent | do not retry with that kind; a forwarder falls back to the legacy forward kind once |

`ReplicateBootstrap` is kind 10, `ReplicateRebuild` kind 24,
`ReplicateMarkedRecords` kind 25, `ReplicateCommittedRecords` kind 26,
`HelloCapable` 27, `HelloCapableOk` 28, `Fence` 29, `FenceOk` 30 and
`ReplicateFetch` 31. A peer
that predates any of them rejects the
kind rather than misreading the body, which is why
each is a new kind rather than a field on `ReplicateRecords`: this protocol
freezes existing body layouts.

`NotLeader` is a distinct kind rather than an error code, because it carries a
routing answer rather than only a reason.

## Acknowledging a forwarded publish

**The ingress broker never acknowledges before the owner has.** A forwarded
publish is acknowledged from the owner's answer and from nothing else.

This overrides `ack_on_commit`, the broker setting that otherwise decides
whether a publish is acknowledged on enqueue or after commit. That setting is a
statement about a *local* write — "accepted into this broker's ingress queue is
good enough" — and for a forward the ingress broker has accepted nothing: the
data is not on its disk, and the owner may still refuse it. So a forward always
takes the commit-ack path, whatever the setting says.

The owner's write is durable-then-answer, so `ForwardPublishOk` means the batch
is as safe on the owner as a local publish would have been on the ingress
broker.

## Retrying a forwarded publish

One question decides it: *could the owner already have applied this batch?* If it
could, a retry is a duplicate rather than a repair, and the ingress broker has no
way to tell the two apart.

| Outcome | Applied? | Retried |
| --- | --- | --- |
| Shed, backoff, unreachable | No — nothing was sent | Yes |
| `NotLeader` | No — the refusal is the evidence | Yes, against the owner it names |
| `StaleRoute`, `Unavailable`, `Overload` | No — refused before writing | Yes |
| `Unauthorized`, `Malformed`, `StorageFailed` | Refused, or tried and failed | No |
| Connection dropped, request timed out | **Unknown** | **No** |

The last row is the important one. The batch may be on the owner's disk, and no
answer is coming. The publish is reported as *indeterminate* rather than failed,
because "failed" is a claim the ingress broker cannot make.

**This is stricter than `AtLeastOnce` allows**, deliberately. A duplicate
produced inside the broker is invisible to the client, which holds the
`request_id` and is the only layer that could deduplicate. A client that wants
the retry can reissue and know that it did.

Retries are bounded by an attempt budget, so a shard being reassigned converges
or fails explicitly instead of chasing `NotLeader` around the cluster. A redirect
back to a `(broker, generation)` the batch has already been sent to is refused
rather than followed: that is a loop, and following it would bounce the batch
between two brokers that disagree.

A redirect to a broker *not* yet tried is followed even when the generation has
not advanced. On a freshly formed cluster every shard sits at generation 0, so a
requester whose routing snapshot has not converged points at the wrong owner
*there* — and the correct owner's redirect is also at generation 0. Refusing
every same-generation redirect made that transient misroute fatal instead of
self-correcting.

## Chains are refused, not relayed

A broker asked for a shard it does not own answers `NotLeader`. It never forwards
onward on the requester's behalf. One request crossing an unbounded chain of
brokers would have unbounded latency and a failure mode nobody can reason about;
the requester holds the decision instead. This applies to a forwarded cache
operation exactly as it does to a forwarded publish — both go through the same
ownership check for that reason.

## The transport

Peers reach each other over a QUIC endpoint of their own. What it guarantees,
and what it refuses:

**Peers authenticate each other, when told how.** With `FELIX_INTERNAL_TLS_CERT`,
`FELIX_INTERNAL_TLS_KEY` and `FELIX_INTERNAL_TLS_CA` set — all three or none —
both ends of every peer connection present a certificate and verify the other's
chains to the CA. The certificate's DNS name is the broker's identity, and it
is checked in both directions: a dialler verifies the listener's certificate
against the node id it meant to reach, so a catalog entry pointing at the wrong
broker fails in the handshake; and the listener checks the node id a peer
claims in `Hello` against the certificate it presented, so a broker holding a
valid certificate for one name cannot speak as another. A peer without a
certificate, with one from another CA, with an expired one, or claiming a name
its certificate does not carry, is refused — the first three in the handshake,
the last with `Unauthorized` and a closed connection. It follows that a node id
has to be a valid DNS name, and startup refuses one that is not.

Rotation is a file swap: the certificate and key are re-read every thirty
seconds and installed for the *next* handshake, and connections already up keep
the identity they were made with, so a rolling renewal — cert-manager's, say —
never drops healthy traffic. The CA bundle is read once; changing trust roots
is a restart. Nothing about the key is ever logged; a load failure names the
variable and the path.

Without the three variables the transport runs **unauthenticated**: the link
is encrypted, brokers present self-signed certificates and accept any, and
anything that can reach the port is a peer. A broker with `FELIX_NODE_ID`
refuses to start in that mode unless `FELIX_INTERNAL_ALLOW_UNAUTHENTICATED=true`
says the operator has put the internal listener on a network only brokers can
reach.

**Connections are pooled and reused.** Repeated requests to one peer share a
connection; several multiplexed streams carry them, because a QUIC stream is
ordered and one large forwarded batch would otherwise hold up every smaller
request behind it. A peer also answers each stream's requests one at a time,
and a forwarded `Quorum` publish can hold one for its whole quorum timeout, so
one more stream per connection carries only the handshake, `Fence` and
`ReplicateFetch`: a promoted leader serves nothing until those are answered,
and must not wait behind forwarded writes.

**Every request terminates.** A connection that drops fails every request
waiting on it at that moment, rather than leaving each to reach its own timeout.
This is what makes the correlation rule above true in practice: a forwarded
publish is never left pending.

**An unhealthy peer cannot consume this broker.** Three bounds, each failing
differently:

| Bound | What crossing it means |
| --- | --- |
| In-flight requests per peer | Requests are shed immediately, not queued. Nothing was sent, so the caller may retry elsewhere at once |
| Reconnect backoff | A peer that refuses connections is redialled on a jittered exponential schedule, and requests arriving inside the window fail without a dial. Jitter matters because every broker notices the same peer restart at the same moment |
| Request timeout | A peer that accepts a request and never answers still releases its waiter |

**A dropped connection and a timeout are not retryable.** The peer may have
applied the write before the answer was lost, so retrying is a duplicate rather
than a repair. Only a shed request — where nothing was sent — is safe to retry
as-is.

Idle connections are closed: rebalancing changes which peers a broker forwards
to, and a connection to one it no longer talks to is a file descriptor and a
keepalive with nothing to do.

## Limits and validation

Every peer-provided field is validated before it is used to allocate:

- Declared payload counts are bounded by what the remaining bytes could hold,
  before any allocation, exactly as the client binary path does.
- String fields are length-prefixed and bounded, and must be valid UTF-8.
- The total body is bounded by the frame length, which is itself bounded.

Peer-provided does not mean trusted. A broker is authenticated, not assumed
correct: a compromised or buggy peer must fail decoding rather than be able to
allocate on demand.
