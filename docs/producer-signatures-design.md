# Producer signatures on records: design

Status: decided, not built. The decisions are at the end. Tracks [#2803](https://github.com/GetFelix/felix/issues/2803),
threat T26 in the NineLives threat model, and the "Records are not signed" and
"Checksums are not signatures" gaps on the
[threat model page](../docs-site/src/content/docs/security/threat-model.md).

## The threat

A record in Felix is only as trustworthy as the brokers that handled it.
Three things a compromised broker can do today:

1. **Inject records as a shard's leader.** The leader assigns offsets and
   replicates entries. Followers append whatever an authorized leader at the
   current term sends them, because that is replication. Honest followers end
   up holding the forged records too.
2. **Inject records as a follower that becomes leader.** A broker holding a
   valid internal certificate for a group member can win an election and then
   do the same as above.
3. **Serve readers wrong data.** Whatever the log holds, the broker a reader
   is connected to can send that reader something else.

The controls that exist do not cover this. Forwarded publishes are checked
against the client's own token, so a broker cannot write to a tenant *on its
own say-so* through forwarding, but a shard's leader is the one doing the
writing. Replication compares bytes against what a follower already holds
(`docs/threat-model-internal.md`, case 2), which stops a broker rewriting
records already replicated and does nothing about records appended past the
tail. Every segment record has a CRC-32, which catches accidents and is
trivial to recompute. The `publisher` a record carries (storage-format bit 27)
is stamped by the broker, so it says who the broker claims sent it.

What a reader needs is a statement from the producer that the broker cannot
make on the producer's behalf: a digital signature, with a key the broker
never holds.

## What a signature can and cannot give

A signature over a batch lets a reader check three things: the producer's key
signed these exact bytes, for this tenant, stream and shard, under this
producer id and sequence number. That is enough to reject an injected record,
a record moved from another stream or tenant, and a modified record.

It cannot show what is *missing*. The broker decides what a reader sees, so it
can still drop records, hide the newest ones, serve an old prefix, show two
readers different subsets, or trim the head of the log. Signed sequence
numbers let a reader notice a gap or a duplicate *within one producer's run
on one shard*, as long as it reads that run without skipping. Across
producers, and across a producer restart that takes a new producer id, the
broker controls the order and the cut-off.

Everything else the threat model lists for brokers also stays: a broker sees
payloads in plaintext, a stolen producer key signs anything, and records the
broker writes itself (generation starts, trims, membership, consumer-group
state, cache and counter records) carry no producer signature.

## The signed statement

All designs below sign the same thing, so the key and format decisions are
shared. A signed batch must come from an idempotent producer
(`publish_idempotent` or the binary idempotent batch), because its producer id
and sequence are what make a replayed batch detectable. The producer signs:

```text
"felix-batch-sig-v1"            domain separation
tenant, namespace, stream       length-prefixed UTF-8
shard                           u32, the one the sequence counts on
producer_id, sequence           u64, u64
count                           u32, records in the batch
h_1 .. h_count                  32-byte hash of each record
```

Each `h_i` is SHA-256 over a canonical encoding of the record: its key (if
any), its headers other than the signature ones, in order, and its payload,
each length-prefixed. Offsets and broker timestamps are not signed: the
producer does not know them when it signs.

The signature is Ed25519, the algorithm Felix tokens already use. A key is
named by a `kid`.

## Where producer keys come from

A reader has to know which public key is allowed to sign for which producing
principal. Two sources, which can coexist:

- **Supplied by the application.** The client library takes a key resolver:
  give it a `kid`, get back a public key and the principal it stands for, or
  nothing. Felix defines the format and does the checking; the application
  decides where keys live. No control-plane change, nothing for the broker to
  lie about, and the operator owns key distribution.
- **Registered in the control plane.** Each principal registers producer
  public keys (stage, activate, retire, as tenant signing keys work today),
  and the control plane publishes a tenant's producer key set the way it
  publishes the JWKS. A reader fetches it from the control plane over TLS,
  never through the broker it is checking. This binds a key to a principal
  through RBAC, at the cost that a compromised control-plane member can
  register a key for anyone. That ties the strength of this design to
  [#2801](https://github.com/GetFelix/felix/issues/2801) and to the metadata
  itself.

Retiring a producer key cannot work like retiring a token key. Records stay
in the log for as long as retention keeps them, so a retired key must stay
verify-only for that long. Revoking a *leaked* key is different: a reader
must refuse records it signed, and since the broker timestamp is not trusted,
the cut-off has to be "every record this key signed", or a signed offset
recorded by the control plane at revocation time.

## Design A: signatures in record headers, checked by readers

The signature rides in reserved record headers, which the wire and storage
already carry byte for byte
([Extended frames and record headers](protocol.md#extended-frames-and-record-headers)).
The broker stores and delivers them without looking inside.

The producer adds a header named `felix.sig` to every record of the batch:
version (1 byte), `kid` (16), producer id (8), sequence (8), the record's
position in the batch (4) and the batch's count (4). The last record's
`felix.sig` also carries the 64-byte signature. A reader collects a batch's
records by producer id and sequence, recomputes the hashes, verifies once, and
then releases the records to the application. A reader that starts in the
middle of a batch reads back `position` records first.

The reader decides the policy, from its own configuration: accept unsigned
records, or refuse them. It must not learn the policy from the broker, since
a broker that strips the headers or claims not to support them would
otherwise turn checking off.

Costs:

- **CPU.** One signature per batch on the producer, one verification per
  batch on every reader, and a SHA-256 pass over the bytes on both sides.
  Ed25519 costs roughly 15 to 20 µs to sign and 30 to 50 µs to verify on one
  core (to be measured on the perf boxes). At 100 records per batch that is
  well under a microsecond per record; with one record per batch it caps one
  core at about 25,000 verified records per second. Fanout multiplies the
  verify cost by the number of verifying readers. Brokers do nothing extra.
- **Bytes.** About 55 bytes of header per record, plus 64 per batch, on the
  wire and on disk. A 100-byte record grows by half.
- **Negotiation.** None new: it uses `EXT_RECORD_HEADERS`, so it needs the
  fleet to have finalized `record_headers` (and `raft_record_headers` on
  Raft). No new flag bit and no storage-format version.
- **Metadata level.** None with application-supplied keys. A key registry
  adds commands at a new level.

What it does not cover, beyond the limits above: consumer groups do not
deliver headers today, so group consumers cannot verify until they do. Kafka
and MQTT producers cannot sign (a Kafka producer interceptor could add the
headers, but nothing standard does). A gateway that publishes for browsers
signs as itself, so readers trust the gateway. Followers and leaders store
injected records happily; only readers that check refuse them.

## Design B: a signature field the brokers check too

The signature becomes part of the record format and the wire, and every
honest broker checks it.

- A stream gets a `signatures` setting (`off`, `required`) in its metadata,
  at a new metadata level.
- The publish batch gets a signature trailer under a new extension bit
  (`EXT_BATCH_SIGNATURE` in the extension word next to `EXT_RECORD_HEADERS`),
  and event batches carry it to readers that offered the same bit.
- Storage gets a format version 10 with a record bit for "the batch's
  signature follows". The v9 batch-piece tag already stores producer id,
  sequence and position on every piece of an idempotent batch, so only the
  last piece grows, by the `kid` and signature. As with publishers (v6) and
  headers (v8), a segment is written at v10 only to hold such a record.
- The leader verifies a batch against the producer key set before it takes
  offsets, and refuses a bad or missing signature on a `required` stream. That
  stops a client holding a stolen token but not the producer's key.
- An honest follower verifies each entry as it applies. It cannot refuse the
  entry: openraft treats an error from log storage as fatal, and a follower
  that refused entries would only stall the shard. So the follower marks the
  range as unverified, raises a metric and an alert, and refuses to serve
  those records to readers. The injection is detected, and still committed.

Costs: the same signature and hash work as Design A on producers and readers,
plus one verification per batch on the leader and on every follower, which at
10,000 batches per second per shard is about a third to half a core per
replica. A new extension bit, a new storage-format version and its golden
vectors, a fleet feature to finalize, a stream metadata field, and key
material that every broker must fetch and keep current, like the JWKS.

What it adds over Design A: brokers refuse unsigned writes, so a stolen token
alone cannot write to a signed stream, and an injection by a compromised
leader is noticed by the honest replicas even if no reader checks. What it
does not add: a compromised leader still commits what it likes, and a
compromised broker still serves its readers whatever it likes. Readers that
care must still verify.

## Design C: replicas attest to the log instead

No producer keys at all. Each replica keeps a running hash of its log and
periodically signs a checkpoint (shard, term, commit index, running hash) with
its internal certificate key. A reader fetches checkpoints from a majority of
the shard's replicas and checks that what its broker served hashes to the
attested value.

This catches one broker serving a reader something other than the committed
log, including omissions below the checkpoint. It does not catch injection by
a compromised leader at all, because injected entries become the committed
log and honest replicas attest to them. It also needs readers to reach several
replicas, trust the internal CA, and wait for a checkpoint before trusting a
record. It covers the "serve wrong data" half of T26 and leaves the "inject"
half open, so it is not a candidate on its own.

## Costs side by side

| | A: headers, readers check | B: format field, brokers check | C: replica checkpoints |
| --- | --- | --- | --- |
| Stops injection reaching a checking reader | Yes | Yes | No |
| Honest replicas notice injection | No | Yes | No |
| Detects a broker omitting records | Within one producer's run | Within one producer's run | Below the latest checkpoint |
| Producer CPU | One sign per batch | One sign per batch | None |
| Broker CPU | None | One verify per batch per replica | Hashing, plus a signature per checkpoint |
| Bytes | About 55 per record, 64 per batch | 80 per batch | None per record |
| Wire | Existing `EXT_RECORD_HEADERS` | New extension bit | New request |
| Storage format | Unchanged | Version 10 | Running hash in the log or recomputed |
| Metadata level | None, or one with a key registry | One, plus the key registry | One |
| Works for Kafka, MQTT, groups | No | No | Yes |

## Migration

Records written before signing is turned on carry no signature. A reader that
requires signatures has to be told where to start requiring them: an offset
per shard, which the operator records when the producers switch. With Design
A that is reader configuration; with Design B it is the offset where the
stream's setting changed, recorded in metadata.

Design A needs nothing from brokers beyond finalized record headers, and old
clients see the signature headers as ordinary headers, or no headers at all
if they did not negotiate them. Design B follows the usual order for a format
change: the fleet feature finalizes, then v10 segments may be written, then
a stream may be set to `required`.

## Decisions

Build Design A, with application-supplied keys, in the Rust client first. It
answers the injection half of T26 for every reader that checks: without the
producer's key, a compromised leader cannot get a forged record past a
checking reader, and no broker, format or negotiation change is needed. Add
the control-plane producer key registry once #2801's custody work is in,
since its strength depends on it, and move consumer groups to carry headers
so group consumers can verify. Design B stays the follow-up if brokers must
refuse unsigned writes. Do not build Design C.

- **Reader-side detection is enough for the first cut.** Brokers refusing
  unsigned writes (Design B) needs a format version and a fleet feature, and
  can follow without changing producers.
- **Keys are supplied by the application.** The registry comes after #2801.
- **Signed batches require idempotent producers.** The producer sequence is
  what stops a replayed batch.
- **About 55 bytes per record is acceptable.** Signing is opt-in, so only
  producers that want it pay.
- **The `felix.` header prefix is reserved.** The broker refuses client
  publishes that set it.

Be clear in the docs about what this buys. Signatures make injected and
altered records detectable. They do not make a compromised broker unable to
hide records, serve stale data, or read payloads.

## Next steps

- [#2968](https://github.com/GetFelix/felix/issues/2968): reserve the `felix.` header prefix (0.8.0)
- [#2969](https://github.com/GetFelix/felix/issues/2969): Rust client signing and reader verification on idempotent producers (0.8.0)
- [#2970](https://github.com/GetFelix/felix/issues/2970): other clients and consumer-group headers (0.9.0)
- [#2971](https://github.com/GetFelix/felix/issues/2971): broker-side checks with storage v10 (0.9.0)
