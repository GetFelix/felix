# Kafka wire compatibility

A Felix broker can serve Kafka clients. With `FELIX_KAFKA_LISTEN` set, the
broker opens a second listener that speaks enough of the Kafka protocol for a
consumer to list topics, look up offsets and fetch records from durable streams,
and for a producer, idempotent or not, to write to them. It has no consumer
groups and no transactions. A consumer that assigns its own partitions and keeps
its own offsets works; anything built on `group.id` and `subscribe()` does not,
and neither does a producer with `transactional.id`.

The user-facing guide, with quick start, use cases and troubleshooting, is
[Kafka compatibility](https://gabloe.github.io/felix/features/kafka/).
This page is the reference: what is implemented, how Felix maps onto Kafka's
model, and why it stops where it does.

The protocol code is in `crates/server/felix-kafka`; the broker wires it up in
`services/felix-broker-service/src/serving/kafka.rs`.

## What works

| API | Versions | Notes |
|---|---|---|
| `ApiVersions` | 0-3 | A request for a newer version gets `UNSUPPORTED_VERSION` in the v0 shape, as KIP-511 requires, so the client retries at one this listener speaks. |
| `SaslHandshake` | 0-1 | v1 works. v0 is listed only because librdkafka will not use SASL without it; a v0 handshake is refused, since its exchange travels outside Kafka framing. |
| `SaslAuthenticate` | 0-2 | SASL/PLAIN. |
| `Metadata` | 0-12 | Topics, partitions, and each partition's leader. |
| `ListOffsets` | 1-7 | Earliest, latest, v7's max-timestamp, and offset-for-time. |
| `Fetch` | 4-12 | Long-polls, woken by the append rather than a timer. |
| `Produce` | 3-9 | v2 record batches, any codec, `acks` 0, 1 and all. See [Produce](#produce). |
| `InitProducerId` | 0-4 | Idempotent producers. A transactional id is refused. |
| `FindCoordinator` | 0-4 | Offered and always refused. See [Consumer groups](#consumer-groups-are-refused) and [Transactions](#transactions-are-refused). |

Fetch v13 and later address topics by id, and Felix streams have names, not ids,
so those versions are not offered. Produce v10 and later add leader hints only
newer clients use; v9 is what librdkafka 2.x settles on.

Tested with kcat 1.7.1 (librdkafka 1.8.2) in
`services/felix-broker-service/tests/kafka_kcat.rs`: listing, consuming every
partition from the beginning and from an offset, offset queries, a fetch woken
by a publish, SASL_SSL and anonymous access, a bad token, a group consumer,
producing with each codec and each `acks`, an idempotent producer, a keyed
produce, and a transactional producer. A three-broker cluster test in
`crates/testing/felix-cluster/tests/routing/kafka_leaders.rs` consumes every
partition of a topic whose shards are led by different brokers, and follows a
shard move. `crates/testing/felix-cluster/tests/failures/kafka_produce.rs`
re-sends an idempotent producer's batches to a promoted leader and to a move's
destination.

> `kcat_consumes_every_partition_from_the_beginning_and_from_an_offset`,
> `kcat_receives_a_record_published_while_its_fetch_waits`,
> `kcat_reads_over_sasl_ssl_and_anonymously_when_allowed`,
> `kcat_joining_a_group_exits_with_a_readable_error`,
> `kcat_produces_and_felix_and_kcat_read_the_same_records`,
> `kcat_produces_with_every_compression_codec`,
> `kcat_produces_with_acks_zero_one_and_all`,
> `kcat_produces_idempotently`,
> `kcat_produces_keyed_records_and_the_value_is_kept`,
> `kcat_transactional_producer_is_refused_readably`,
> `an_idempotent_kafka_producer_lands_each_record_once_through_failover_and_a_move`.

## What does not work

- Consumer groups: `group.id` with `subscribe()`, committed offsets, and
  everything built on them. That rules out Kafka Connect, Kafka Streams, ksqlDB,
  Debezium and MirrorMaker.
- Transactions: `transactional.id`, and exactly-once pipelines built on it.
- Legacy v0 and v1 message sets. Every client since Kafka 0.11 writes v2.
- In-memory streams. Only durable streams are listed.
- Fetch sessions (KIP-227). The listener answers with session id 0, which tells
  the client to send full fetches every time.
- Leader epochs. Reported as -1, meaning unknown.
- Keys, headers and producer timestamps. A record has a value and the broker's
  append time only; a produced key and headers are dropped.
- OAUTHBEARER and re-authentication. A token is checked once, when the
  connection authenticates.
- Topic ids (Fetch v13 and later).
- Admin APIs. Only the APIs in the table under [What works](#what-works) are served, so
  `CreateTopics`, `DeleteTopics`, `CreatePartitions`, `DescribeConfigs` and
  topic auto-creation are not available. A topic is a durable stream created
  through the control plane, and its partition count is its shard count.
- Namespaces containing a dot. The topic name splits on the first dot, so such
  a namespace cannot be addressed and its streams are not listed.
- Log compaction and Kafka retention settings. A tombstone (null value) is
  stored as an empty payload and deletes nothing.
- SASL mechanisms other than PLAIN (no SCRAM).
- Tested clients: kcat 1.7.1 (librdkafka 1.8.2). The Java client is untested.

Two things work but differ from Kafka: `acks=all` on a `Leader` stream waits
for the leader only (see [acks](#acks)), and an idempotent re-send older than
the last 64 batches per producer and partition is answered
`DUPLICATE_SEQUENCE_NUMBER` without an offset (see [Idempotent
producers](#idempotent-producers)).

## The mapping

**Topics.** A topic is `<namespace>.<stream>`, split on the first dot, so
`orders.created` is stream `created` in namespace `orders`. The tenant never
appears in the topic; it comes from the credential. With
`FELIX_KAFKA_DEFAULT_NAMESPACE` set, a topic with no dot is looked up in that
namespace.

A stream is listed only if it is durable. Kafka consumers address records by
offset and expect them to still be there later; an in-memory stream keeps only a
bounded replay window, so offsets would point at records that are gone. A few
durable streams are also left out because their name cannot survive the round
trip: a namespace containing a dot would read back as a different stream, names
with characters outside `[A-Za-z0-9._-]` are not legal Kafka topic names, and
topics longer than 249 characters are refused by clients.

**Partitions.** Partition N is shard N, and a topic has as many partitions as
its stream has shards.

**Offsets.** Kafka offsets are Felix's own log offsets, untranslated. They are
contiguous per shard and start at 0, which is exactly the shape Kafka assumes, so
there is no translation table to keep durable. The high watermark is the shard
log's tail. The log start offset is the oldest retained offset, and retention
raises it. A fetch below the log start or past the tail gets
`OFFSET_OUT_OF_RANGE` and the client applies its `auto.offset.reset`.
Offset-for-time is a binary search over the records' append timestamps.

**Records.** The value is the Felix payload. There is no key and there are no
headers. The timestamp is the broker's append time in milliseconds, reported as
`CreateTime` on fetch. A produced record's key, headers and timestamp are
dropped (see [Produce](#produce)).

**Brokers and replicas.** The Kafka broker id is a stable FNV-1a hash of the
Felix node id, folded to a non-negative `i32`, so every broker names every other
the same way. A single broker outside a cluster is node `felix`. The cluster id
is `felix-<node id>`. A partition's replicas are the shard's replica set, and
its ISR is the leader alone: Felix does not track follower catch-up in the sense
Kafka's ISR means, and listing followers as in sync would be a claim nothing
checks.

**Advertised addresses.** Each broker registers its Kafka address
(`FELIX_KAFKA_ADVERTISE_ADDR`, or the listen address) with the control plane as
the node's `kafka_addr`. That is what lets any broker answer Metadata with the
address of every partition's leader.

**Authentication.** SASL/PLAIN, with the tenant id as the username and a Felix
token as the password. It is the same token a Felix client sends in its QUIC
`Auth` frame, checked by the same code. An authzid naming a different tenant is
refused. Reading a topic needs `stream.subscribe` on the stream, which is the
check a Felix subscribe makes; producing needs `stream.publish`, the check a
Felix publish makes. `InitProducerId` needs only an authenticated connection.

### Errors

| Felix condition | Kafka error | What the client does |
|---|---|---|
| Stream, namespace or tenant does not exist, or the stream is not durable | `UNKNOWN_TOPIC_OR_PARTITION` | Reports the topic as missing |
| Partition number beyond the shard count | `UNKNOWN_TOPIC_OR_PARTITION` | Reports the partition as missing |
| Principal lacks `stream.subscribe` (read) or `stream.publish` (produce), or the connection is not authenticated | `TOPIC_AUTHORIZATION_FAILED` | Reports it; fatal for that topic |
| Token rejected, or authzid names another tenant | `SASL_AUTHENTICATION_FAILED`, then the connection closes | Reports the message and gives up |
| Shard unassigned, opening or moving, or its leader advertises no Kafka address | `LEADER_NOT_AVAILABLE`, leader -1 (in Metadata) | Retries Metadata |
| This broker does not lead the partition (moved, failed over, unavailable) | `NOT_LEADER_OR_FOLLOWER` | Refreshes Metadata, goes to the new leader |
| Offset below the oldest retained record or past the tail | `OFFSET_OUT_OF_RANGE` | Applies `auto.offset.reset` |
| Storage failure while reading | `KAFKA_STORAGE_ERROR` | Retries |
| Any group API | `GROUP_AUTHORIZATION_FAILED` | Fails the group consumer with the message |
| A transaction: coordinator lookup, `InitProducerId` with a transactional id, a transaction API, or a transactional batch | `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` | Fatal for the producer, with the message |
| `InitProducerId` on an unauthenticated connection | `CLUSTER_AUTHORIZATION_FAILED` | Fatal for the producer |
| Produce to a partition this broker does not lead, or it lost the lease or the shard began moving before the write | `NOT_LEADER_OR_FOLLOWER` | Refreshes Metadata, re-sends to the leader |
| `acks=all` on a `Quorum` stream and no majority in time | `REQUEST_TIMED_OUT` | Retries; idempotence keeps the retry from writing twice |
| Leadership moved after the write and before a majority held it | `NOT_LEADER_OR_FOLLOWER` | Re-sends to the new leader |
| v0 or v1 message set | `UNSUPPORTED_FOR_MESSAGE_FORMAT` | Fails the produce |
| Bad CRC, truncated batch, unknown codec | `CORRUPT_MESSAGE` | Retries, then fails |
| Batch decompresses past 16 MiB | `MESSAGE_TOO_LARGE` | Fails the produce |
| Idempotent batch ahead of the next sequence | `OUT_OF_ORDER_SEQUENCE_NUMBER` | Fatal for the idempotent producer |
| Idempotent batch from a producer the partition's log does not know, not at sequence 0 | `UNKNOWN_PRODUCER_ID` | Takes a new producer id and carries on |
| Idempotent re-send older than the offsets the log remembers | `DUPLICATE_SEQUENCE_NUMBER` | Counts the batch delivered, without an offset |

The authorization check runs before the existence check, so a principal that may
not read a stream cannot learn whether it exists. A topic the principal may not
read is also left out when Metadata lists every topic. An unauthenticated
connection sees no topics, unless `FELIX_KAFKA_ANONYMOUS_TENANT` is set, in which
case it reads every stream of that tenant.

The SASL failure message says what to do: `Felix: token rejected: ... Use the
tenant id as the username and a Felix token as the password.`

## Fetching

A fetch that finds fewer than `min_bytes` waits up to `max_wait_ms`, capped at
30 seconds. It is woken by the shard's append notification the moment a publish
commits, so a waiting consumer sees a record as soon as it is durable rather
than on the next poll tick. A partition error ends the wait at once, and so does
broker shutdown. At least one record is returned even if it is larger than
`max_bytes`, as Kafka does, so an oversized record cannot stall a consumer.

Requests on one connection are answered in order, one at a time, which is also
Kafka's rule. A request frame larger than 8 MiB closes the connection.

## Leadership and moves

Metadata names each partition's leader, which is the shard's leader. A client
sends fetches for a partition to that broker. When the shard moves, or its
leader fails and a replica takes over, the old broker answers the next fetch
with `NOT_LEADER_OR_FOLLOWER`; librdkafka refreshes Metadata and carries on at
the new leader. Offsets are the log's, and the new leader holds the same log, so
the consumer resumes at the offset it asked for. Nothing is skipped or repeated;
the client sees a short pause.

```mermaid
sequenceDiagram
    participant C as Kafka consumer
    participant A as Broker A
    participant B as Broker B
    C->>A: Metadata for orders.created
    A-->>C: partition 1 is led by broker A
    C->>A: Fetch partition 1 from offset 42
    A-->>C: records 42 to 57
    Note over A,B: shard 1 moves from A to B
    C->>A: Fetch partition 1 from offset 58
    A-->>C: NOT_LEADER_OR_FOLLOWER
    C->>B: Metadata for orders.created
    B-->>C: partition 1 is led by broker B
    C->>B: Fetch partition 1 from offset 58
    B-->>C: records 58 onward
```

## Consumer groups are refused

`FindCoordinator` is always answered with `GROUP_AUTHORIZATION_FAILED` and the
message `Felix has no Kafka consumer groups; assign partitions instead. See
docs/kafka-compatibility.md`. The other group APIs (`JoinGroup`, `SyncGroup`,
`Heartbeat`, `LeaveGroup`, `OffsetCommit`, `OffsetFetch`) are not advertised,
and get the same answer if a client sends one anyway.

The code was chosen by trying it. The protocol has no way to say "this broker
has no coordinator". Leaving `FindCoordinator` unanswered, as the first
prototype did, makes kcat print `Waiting for group rebalance` and hang there
with no error, which is the worst way to fail: the person holding the client
cannot tell a missing feature from a slow cluster. librdkafka treats
`GROUP_AUTHORIZATION_FAILED` as fatal for the group and hands the message to the
application. kcat 1.7.1 with `-G` prints:

```console
% Waiting for group rebalance
% ERROR: Consumer error: FindCoordinator response error: Felix has no Kafka consumer groups; assign partitions instead. See docs/kafka-compatibility.md
```

and exits non-zero. Other clients may word it differently; the message is the
part that carries through.

### Why there is no coordinator

Kafka's group protocol is `FindCoordinator`, `JoinGroup`, `SyncGroup`,
`Heartbeat`, `LeaveGroup`, `OffsetCommit`, `OffsetFetch`, assignment
strategies, generation ids, and a rebalance protocol whose edge cases are a
decade of bug reports. Felix has the storage half (durable per-shard cursors)
and deliberately none of the coordination half. Building it as a compatibility
layer means building a rebalance protocol Felix does not want, and then owning
its behaviour across Kafka client versions. That obligation would outlive
whoever wrote it.

Other Kafka-compatible systems show where the effort goes:

- **Redpanda** reimplemented the broker natively in C++ and treats the Kafka
  protocol as its only protocol. Compatibility is the product, not a layer.
- **AutoMQ** keeps Kafka's own broker code and replaces the storage beneath it,
  which sidesteps the protocol entirely. The coordinator is Kafka's.
- **WarpStream** reimplemented the protocol including the coordinator, and its
  published engineering writing is mostly about the coordinator.
- **Kafka-on-Pulsar** built the coordinator on top of Pulsar and found the group
  and transaction semantics to be the long tail.

Each either adopted Kafka's coordinator wholesale or spent most of its effort
rebuilding it, and none got the ecosystem without it.

In Felix, sharing work between consumers is a Felix consumer group
([Queues and consumer groups](https://gabloe.github.io/felix/features/queues/)),
used through a Felix client. Reading with a checkpoint is a resumable
subscription: every delivered event carries its offset, and a subscribe can
start from one.

## Produce

A produce is written through the broker's own publish path on the shard's
leader. Per partition, in this order:

1. The topic must name a durable stream the principal may publish to, and the
   partition must be one of its shards. Authorization comes before existence,
   as for reads.
2. The records must decode: v2 record batches, uncompressed or compressed with
   gzip, snappy (raw, as librdkafka sends it, or xerial-framed, as the Java
   client does), lz4 or zstd. Decompression stops at 16 MiB a batch, so a small
   compressed batch cannot make the broker allocate without bound.
3. The cluster must admit the write the way it admits a QUIC publish: the
   broker holds its lease, routing sends the shard here, and the shard's write
   fence is open. The fence place is held until the write is durable, so a move
   waits for it. Anything else is `NOT_LEADER_OR_FOLLOWER`; a Kafka client goes
   to the leader itself, so nothing is forwarded.
4. Each batch is published; an idempotent one through the log's producer
   sequences (below).
5. With `acks=all`, the answer waits for the stream's consistency.

A partition's answer carries the first offset written, the log start offset,
and the broker's append time as `log_append_time_ms`, which tells the client
that the timestamp it sent was not kept.

Requests on a connection are answered one at a time and in order, which is
Kafka's rule, so one connection has one produce in flight at a time at the
broker even if the client pipelines several.

### Keys, headers and timestamps

Felix records are a payload and an append time. A produced record's key has
already chosen the partition by the time it arrives, so records with the same
key still land on the same shard in order; the key is then dropped. Headers are
dropped too, rather than refused, because producers add them without being asked
(tracing interceptors, for one) and refusing would break them for data nobody
reads here. Both are counted in `felix_kafka_produce_dropped_total{field}`. A
null value is stored as an empty payload. Carrying keys and headers would need
a new record format, and nothing reading Felix streams uses them today.

### acks

| `acks` | Waits for |
|---|---|
| `0` | Nothing. The records are written and no answer is sent. |
| `1` | The leader's write, as durable as the stream's fsync policy makes it. |
| `all` | The stream's consistency: a majority of the replica set on a `Quorum` stream. On a `Leader` stream that is the leader's write, the same as `acks=1`. |

`acks=all` never claims more than the stream gives. A `Leader` stream
acknowledges on the leader by design, and asking Kafka-style for "all" does not
change the stream.

### Quotas

A produce counts against its tenant's publish quota
(`FELIX_TENANT_PUBLISH_*`), the same budget the tenant's QUIC publishes draw
on. Over quota, the produce is still written and answered, with
`throttle_time_ms` set; the connection then reads nothing more until that time
has passed. That is how a Kafka broker enforces its own quotas, and a client
that honours `throttle_time_ms` waits the same window rather than on top of it.
The hold is at most five seconds, well inside a producer's request timeout.

### Idempotent producers

`InitProducerId` without a transactional id returns a Felix producer id (the
same kind a Felix idempotent producer gets, with the sign bit cleared) at epoch
0. The id is not recorded anywhere: nothing on the broker needs to remember
issuing it.

Kafka numbers records, not batches. A producer's first batch to a partition has
base sequence 0; the next starts at 0 plus the first batch's record count. Felix
producers number batches, and the log expects each batch at the previous one
plus one. Rather than teach the log a second rule, the listener stores each
record of an idempotent Kafka batch as a one-record Felix producer batch: record
`i` of a batch with base sequence `s` carries sequence `s + i`. The log's "one
more than the last" is then exactly Kafka's rule, and everything the log already
does for Felix producers applies unchanged
(`Broker::publish_records_idempotent`):

- **The state is in the log.** Each record carries its producer id and sequence
  (the producer mark in `docs/storage-format.md`). Replication ships the marks
  with the records, and a promoted replica, a move's destination or a restarted
  broker rebuilds each producer's place from them. A batch re-sent after a
  failover is answered by the new leader with the offset it was first written
  at, and nothing is written.
- **A batch cut short is finished.** When a leader died after shipping part of
  a batch, the re-send writes only the records the new leader lacks.
- **Refusals.** A base sequence past what the producer owes is
  `OUT_OF_ORDER_SEQUENCE_NUMBER`. A producer the log holds nothing from,
  sending anything but sequence 0, is `UNKNOWN_PRODUCER_ID`. A re-send older
  than the log's window (the last 64 batches per producer and shard) is
  `DUPLICATE_SEQUENCE_NUMBER`, which librdkafka and the Java client count as
  delivered.
- **Wrapping.** Kafka sequences wrap to 0 after 2^31 - 1: a batch with base
  sequence 0 right after a record at `i32::MAX` is in order, and a batch's
  record sequences are computed with wrapping arithmetic, so one batch can
  carry `i32::MAX - 1`, `i32::MAX`, 0, 1. The log counts in 64 bits and never
  wraps, so a Kafka sequence is taken as the 64-bit count nearest to the one
  the producer owes (`lift` in `per_record.rs`). A re-send from either side of
  the wrap is then a duplicate, and a batch past the next sequence is
  `OUT_OF_ORDER_SEQUENCE_NUMBER`, exactly as away from the wrap. A sequence
  more than 2^30 behind what the producer owes reads as ahead and is refused
  as a gap; Kafka refuses it as out of order too.

The epoch is always 0. A producer that would bump its epoch (KIP-360) asks
`InitProducerId` again and gets a new id instead, which restarts its sequences
the same way a bump would. Batches carrying some other epoch are not checked
against it.

The cost is a 20-byte producer tag on every record an idempotent Kafka producer
writes, where a Felix producer pays it once per batch.

Tested in `crates/server/felix-broker/src/broker/publish/per_record/tests.rs`
(re-sends to the same log, a restarted one and a replica shipped part of a
batch), end to end in `kafka_produce.rs` (above), and at the wrap in
`crates/server/felix-kafka/src/api/tests/produce.rs`
(`a_sequence_wraps_to_zero_after_i32_max`, `a_batch_spans_the_sequence_wrap`).

## Transactions are refused

Felix has no transaction coordinator, for the same reasons it has no group
coordinator. A transactional producer starts by asking `FindCoordinator` for its
transaction coordinator (key type 1); that is answered with
`TRANSACTIONAL_ID_AUTHORIZATION_FAILED` and the message `Felix does not support
Kafka transactions; unset transactional.id. See docs/kafka-compatibility.md`.
`InitProducerId` with a transactional id, a produce carrying one, a
transactional batch, and `AddPartitionsToTxn`, `AddOffsetsToTxn`, `EndTxn` and
`TxnOffsetCommit` (not advertised) get the same code.

librdkafka treats the code as fatal and reports it at once. kcat 1.7.1 with
`-X transactional.id=...` prints:

```console
% Using transactional producer
% ERROR: init_transactions(): Failed to find transaction coordinator: sasl_plaintext://host:9092/1480824465: Broker: Transactional Id authorization failed: Felix does not support Kafka transactions; unset transactional.id. See docs/kafka-compatibility.md
```

and exits non-zero.

## Configuration

All of these are read by the broker. The listener is off unless
`FELIX_KAFKA_LISTEN` is set.

| Variable | Default | Meaning |
|---|---|---|
| `FELIX_KAFKA_LISTEN` | unset | `ip:port` to listen on. Unset turns the listener off. |
| `FELIX_KAFKA_ADVERTISE_ADDR` | the listen address | `host:port` clients are told to connect to. A hostname is fine. Registered as the node's `kafka_addr`. |
| `FELIX_KAFKA_TLS` | `true` | TLS with the broker's client-facing certificate, for `SASL_SSL`. `false` serves `SASL_PLAINTEXT`, and tokens cross the network in clear text. |
| `FELIX_KAFKA_ANONYMOUS_TENANT` | unset | Development switch. An unauthenticated connection reads and writes every stream of this tenant. |
| `FELIX_KAFKA_DEFAULT_NAMESPACE` | unset | Namespace for topic names without a dot. |
| `FELIX_KAFKA_MAX_CONNECTIONS` | `1024` | Connections served at once. Extra ones are closed on arrival. |
| `FELIX_KAFKA_MAX_CONNECTIONS_PER_IP` | `128` | Connections one source IP may hold, checked before the total. `0` is unlimited. |
| `FELIX_KAFKA_AUTH_TIMEOUT_MS` | `10000` | How long a connection has to finish SASL before it is closed. Until then each request is capped at 64 KiB, which is far more than `ApiVersions` and the SASL exchange need. |

With TLS on, the listener serves the same certificate the QUIC listeners do:
`FELIX_TLS_CERT` and `FELIX_TLS_KEY` when set, with `FELIX_TLS_CLIENT_CA`
requiring a client certificate on this listener too (librdkafka's
`ssl.certificate.location` and `ssl.key.location`). With
`FELIX_TLS_CLIENT_CERT_BIND_SUBJECT=true` as well, SASL/PLAIN refuses a token
whose `sub` that certificate was not issued to, as the QUIC listeners do.
Point `ssl.ca.location` at
the CA that issued it. Without them the broker generates a self-signed
certificate named `localhost`, which `FELIX_TLS_CERT_EXPORT` writes out for
clients to trust; librdkafka 2.0 and later verify hostnames and need
`ssl.endpoint.identification.algorithm=none` with that one.

The metrics are listed on the
[observability page](https://gabloe.github.io/felix/features/observability/):
`felix_kafka_connections`, `felix_kafka_connections_total`,
`felix_kafka_requests_total{api,error}`, `felix_kafka_refused_total{reason}`,
`felix_kafka_fetch_records_total`, `felix_kafka_fetch_bytes_total`,
`felix_kafka_fetch_waits_total{outcome}`, `felix_kafka_fetch_wait_seconds`,
`felix_kafka_produce_records_total`, `felix_kafka_produce_bytes_total`,
`felix_kafka_produce_duplicate_records_total`,
`felix_kafka_produce_errors_total{error}` and
`felix_kafka_produce_dropped_total{field}`.

## Design choices

**A listener in every broker, not a gateway.** Kafka's model is already Felix's:
each partition has one leader, Metadata names it, and clients go to it directly.
Embedding the listener in the broker means Metadata can simply report where each
shard lives, and a fetch is served by the broker holding the log. A gateway would
have to proxy every fetch to the right broker and would be another thing to run.

**Fetch reads the shard log directly.** A Kafka fetch is offset-addressed, and
so is the shard log's `read_from`, the same call replication uses to catch a
follower up. A gateway built on the Felix client would instead hold a
subscription per partition and reopen it on every seek, which is the wrong shape
for a protocol where the client names the offset on every request.

**Durable streams only.** Offsets are only meaningful if the records they name
stay readable, so in-memory streams are not exposed rather than exposed with
offsets that expire under the consumer. The same holds for producing: an
idempotent producer's sequences live in the log, and an in-memory stream has
none.

**Produce goes through the broker's publish path.** Offsets, durability,
replication and producer sequences are the broker's, not re-implemented in the
listener, so a Kafka producer's records are indistinguishable from a Felix
client's once written, and the idempotence guarantee is the log's.

**Refuse loudly.** A group consumer or a transactional producer gets a real
error with a sentence saying why, rather than a hang or a silent drop. The spike that
preceded this work found that a partial implementation that fails deep inside a
rebalance is worse than none, because the failure is unreadable to the person
holding it.
