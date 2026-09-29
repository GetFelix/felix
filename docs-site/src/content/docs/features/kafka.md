---
title: "Kafka Compatibility"
description: "Read durable Felix streams with Kafka consumers that assign their own partitions, and write to them with Kafka producers, idempotent ones included."
---

A Felix broker can serve Kafka clients. Turn on the Kafka listener and a durable
stream shows up as a Kafka topic. `kcat` or any librdkafka-based client can
list it, look up offsets, read records from it and write records to it. The Java
client speaks the same protocol but has not been tested against Felix yet. The
offsets are the same ones a Felix client sees, and a Kafka producer's records go
through the same publish path as a Felix client's.

Two things are missing, on purpose: consumer groups and transactions. A consumer
that assigns its own partitions and keeps its own offsets works; one that sets
`group.id` and calls `subscribe()` gets an error saying Felix has no groups. A
producer works, idempotent or not; one that sets `transactional.id` gets an
error saying Felix has no transactions. Between them, those rule out Kafka
Connect, Kafka Streams, ksqlDB, Debezium and MirrorMaker.

The reference for the protocol side, including every error code and why groups
and transactions are refused, is
[`docs/kafka-compatibility.md`](https://github.com/gabloe/felix/blob/main/docs/kafka-compatibility.md).

## What works

- Listing topics and partitions (`Metadata`), with each partition's leader.
- Earliest and latest offsets, and the offset for a point in time (`ListOffsets`).
- Fetching from any offset, including long-polling at the tail. A waiting fetch
  is woken the moment a publish commits.
- SASL/PLAIN over TLS, with a Felix token as the password.
- Following a partition to its new leader when a shard moves or fails over.
- Producing, with any codec a producer picks (gzip, snappy, lz4, zstd), and
  `acks` of 0, 1 or all.
- Idempotent producers (`enable.idempotence=true`, the default in the Java
  client since 3.0). A batch re-sent after a failover or a move is recognised by the
  new leader and not written twice.

Tested with kcat 1.7.1 (librdkafka 1.8.2), on a single broker and on a
three-broker cluster whose shards are led by different brokers. The idempotent
producer is also tested across a leader failover and a shard move.

## Quick start

### 1. Turn the listener on

The listener is off unless `FELIX_KAFKA_LISTEN` is set. Only durable streams are
served, so the broker needs durable storage too:

```bash
export FELIX_DURABLE_STORAGE_DIR=/var/lib/felix
export FELIX_KAFKA_LISTEN=0.0.0.0:9092
export FELIX_KAFKA_ADVERTISE_ADDR=broker-1.internal:9092
export FELIX_TLS_CERT_EXPORT=/etc/felix/felix-ca.pem
```

`FELIX_KAFKA_ADVERTISE_ADDR` is the address clients are told to connect to. Set
it whenever the listen address is not reachable as written, which is always the
case for `0.0.0.0`. `FELIX_TLS_CERT_EXPORT` writes the broker's certificate out
so clients can trust it; copy that file to wherever kcat runs.

### 2. Name the topic

A topic is `<namespace>.<stream>`. Stream `created` in namespace `orders`,
created with `durable: true`, is topic `orders.created`. The tenant is not in
the name; it comes from the username you connect with.

### 3. Get a token

The password is an ordinary Felix token, the same one a Felix client uses. It
needs `stream.subscribe` on the streams you want to read and `stream.publish` on
the ones you want to write. Exchange an OIDC token for one at the control plane:

```bash
FELIX_TOKEN=$(curl -s -X POST "https://controlplane:8443/v1/tenants/t1/token/exchange" \
  -H "Authorization: Bearer $OIDC_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"requested": ["stream.subscribe", "stream.publish"], "resources": ["stream:t1/orders/*"]}' \
  | jq -r .felix_token)
```

See [Security](/felix/features/security/) for how tokens and permissions work.

### 4. Connect

With TLS on (the default), connect with `SASL_SSL`. The username is the tenant
id:

```bash
kcat -b broker-1.internal:9092 \
  -X security.protocol=SASL_SSL \
  -X ssl.ca.location=felix-ca.pem \
  -X sasl.mechanisms=PLAIN \
  -X sasl.username=t1 \
  -X sasl.password="$FELIX_TOKEN" \
  -L
```

The commands below leave those `-X` options out for brevity; every one of them
needs them. Putting them in a file and passing `-F kcat.conf` saves typing:

```properties
bootstrap.servers=broker-1.internal:9092
security.protocol=SASL_SSL
ssl.ca.location=felix-ca.pem
sasl.mechanisms=PLAIN
sasl.username=t1
```

The certificate is the one the QUIC listeners serve. With `FELIX_TLS_CERT`
set, point `ssl.ca.location` at the CA that issued it and connect by a name it
carries. Without it, the broker's generated self-signed certificate is named
`localhost`: librdkafka 2.0 and later check the hostname, so add
`-X ssl.endpoint.identification.algorithm=none` there. librdkafka before 2.0
does not check it.

If the broker runs with `FELIX_KAFKA_TLS=false`, use `SASL_PLAINTEXT` and drop
`ssl.ca.location`. The token then crosses the network in clear text, so keep
that to networks you trust.

### Anonymous access for development

On a laptop, `FELIX_KAFKA_ANONYMOUS_TENANT=t1` lets a connection that does not
authenticate read and write every stream of tenant `t1`. With TLS off as well,
plain kcat works:

```bash
FELIX_KAFKA_TLS=false FELIX_KAFKA_ANONYMOUS_TENANT=t1 FELIX_KAFKA_LISTEN=127.0.0.1:9092 felix-broker

kcat -b 127.0.0.1:9092 -L
echo hello | kcat -b 127.0.0.1:9092 -P -t orders.created
kcat -b 127.0.0.1:9092 -C -t orders.created -o beginning -e -q -f '%p:%o:%s\n'
```

Leave it unset anywhere else. It gives read and write access to a whole tenant
to anyone who can reach the port.

### 5. Read and write

```bash
# Every partition from the start, then exit
kcat -C -t orders.created -o beginning -e -q -f '%p:%o:%s\n'

# Partition 1 from offset 42
kcat -C -t orders.created -p 1 -o 42 -e

# One record per line of input, to partition 0
printf 'order-1\norder-2\n' | kcat -P -t orders.created -p 0
```

## Use cases

### Looking at a stream during an incident

kcat is useful when you want to see what is actually in a stream without writing
a Felix client. Latest offset of partition 1:

```console
$ kcat -Q -t orders.created:1:-1
orders.created [1] offset 3
```

The last 10 records of every partition (a negative offset counts back from each
partition's end):

```bash
kcat -C -t orders.created -o -10 -e -f '%p:%o %T %s\n'
```

`%T` is the record's timestamp, which is the time the broker appended it, in
milliseconds.

Everything since a point in time, found with an offset-for-time lookup:

```bash
kcat -C -t orders.created -o s@1727200000000 -e
```

Following new records as they are published:

```bash
kcat -C -t orders.created -o end
```

A fetch waiting at the tail is woken by the publish itself, so records appear as
soon as they are durable rather than on a polling interval.

Where it stops: only durable streams are visible. An in-memory stream does not
appear as a topic at all; read it with a Felix subscriber instead.

### Feeding an existing Kafka-consuming program

A program that already consumes from Kafka can read a Felix stream if it
assigns partitions itself and stores its own offsets. Partition N is shard N,
and the offsets are Felix's log offsets, so a stored offset means the same thing
to Felix and to the program.

A sketch with Python's `confluent-kafka` (client configuration only; kcat is
what is tested):

```python
from confluent_kafka import Consumer, TopicPartition

consumer = Consumer({
    "bootstrap.servers": "broker-1.internal:9092",
    "security.protocol": "SASL_SSL",
    "ssl.ca.location": "felix-ca.pem",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "t1",
    "sasl.password": felix_token,
    "enable.auto.commit": False,
})

# Where each partition left off, from your own store.
start = load_offsets()  # e.g. {0: 1200, 1: 980}
consumer.assign([TopicPartition("orders.created", p, o) for p, o in start.items()])

while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue
    handle(msg.value())
    save_offset(msg.partition(), msg.offset() + 1)
```

Leave `group.id` out if your client allows it. A librdkafka consumer that has
one may ask for a group coordinator, and Felix refuses that request; the refusal
surfaces as an error from `poll()` naming the reason. Some client versions
insist on a `group.id`: that has not been tested here, so check that fetching
carries on past that error before relying on it.

The same in Java, as a sketch. Without `group.id` a `KafkaConsumer` cannot
commit offsets, which is what you want here, since there is nowhere to commit
them to:

```java
Properties props = new Properties();
props.put("bootstrap.servers", "broker-1.internal:9092");
props.put("security.protocol", "SASL_SSL");
props.put("ssl.truststore.type", "PEM");
props.put("ssl.truststore.location", "felix-ca.pem");
props.put("ssl.endpoint.identification.algorithm", "");
props.put("sasl.mechanism", "PLAIN");
props.put("sasl.jaas.config",
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    + "username=\"t1\" password=\"" + felixToken + "\";");
props.put("enable.auto.commit", "false");
props.put("key.deserializer", ByteArrayDeserializer.class.getName());
props.put("value.deserializer", ByteArrayDeserializer.class.getName());

KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
TopicPartition p0 = new TopicPartition("orders.created", 0);
consumer.assign(List.of(p0));
consumer.seek(p0, loadOffset(0));
```

Records have a value and a timestamp. There is no key and there are no headers,
so a pipeline that routes on the Kafka key needs the value parsed instead.

Where it stops: anything that manages its own consumption through a group. Kafka
Connect, Kafka Streams, ksqlDB, Debezium and MirrorMaker all do, and none of
them will run. To split a stream's records across several workers inside Felix,
use a [Felix consumer group](/felix/features/queues/) through a Felix client. To
checkpoint and resume, a Felix subscription can start from an offset and every
event it delivers carries one.

### Piping records into a script

kcat's `-e` exits at the end of the log and `-f` formats each record, which is
enough for quick shell work:

```bash
# How many records are there right now?
kcat -C -t orders.created -o beginning -e -q -f '\n' | wc -l

# Large orders, assuming JSON payloads
kcat -C -t orders.created -o beginning -e -q -f '%s\n' | jq -c 'select(.amount > 1000)'

# One partition's records to a file, with offsets
kcat -C -t orders.created -p 0 -o beginning -e -q -f '%o\t%s\n' > partition-0.tsv
```

Where it stops: kcat reads whatever it is given, so large streams are best
bounded with `-o` and `-c` rather than read from the beginning.

### Feeding Felix from Kafka producers you already have

A service that already writes to Kafka can write to Felix by changing its
bootstrap servers and credentials. Its records land in the stream's log at the
next offsets, next to whatever Felix clients publish, and Felix subscribers,
consumer groups and Kafka consumers all read them.

```python
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "broker-1.internal:9092",
    "security.protocol": "SASL_SSL",
    "ssl.ca.location": "felix-ca.pem",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "t1",
    "sasl.password": felix_token,
    "enable.idempotence": True,
    "compression.type": "zstd",
})
producer.produce("orders.created", value=order_json, key=order_id)
producer.flush()
```

That is a sketch; kcat is what is tested. Three things change on the way in:

- **The key picks the partition and is then dropped.** The client hashes it to
  choose a partition, which is a shard, so records with the same key still land
  on the same shard in order. The key itself is not stored: Felix records do
  not have one, and a consumer reading them back sees no key. If a consumer
  needs it, put it in the value.
- **Headers are dropped.** Tracing headers added by an interceptor are the usual
  case. Nothing refuses them, since refusing would break producers that add
  headers without being asked, but they are counted in
  `felix_kafka_produce_dropped_total{field="headers"}` so you can see it
  happening.
- **The timestamp is the broker's.** A Felix record's timestamp is when the
  broker appended it. The producer's timestamp is not kept, and the produce
  answer says so by carrying the append time, which librdkafka then reports as
  the message's timestamp.

A null value (a Kafka tombstone) is stored as an empty payload. Felix does not
compact, so a tombstone deletes nothing.

Where it stops: transactional producers. See
[Transactions are refused](#transactions-are-refused).

### Moving producers over one at a time

Because Felix speaks the producer side of the protocol, a migration does not
need every writer rewritten at once. Point one producer at Felix, check its
records with `kcat -C` or a Felix subscriber, then move the next. Producers
that are rewritten later against a Felix client write to the same stream, and
readers cannot tell the difference except by the missing keys.

Two things to settle before moving a producer:

- **Partition count.** A topic has one partition per shard. A producer that
  hard-codes partition numbers needs them to be below the stream's shard
  count, or it gets `UNKNOWN_TOPIC_OR_PARTITION`.
- **Permissions.** The token needs `stream.publish` on the stream. Without it
  the producer gets `TOPIC_AUTHORIZATION_FAILED`, and nothing is written.

### What idempotence means here

A Kafka producer with idempotence on (the default in the Java client since
3.0; librdkafka needs `enable.idempotence=true`) asks the broker for a producer id and numbers every record it
sends to a partition. If it sends a batch, loses the answer, and sends it again,
the broker can tell and does not write it twice.

Felix keeps that promise, including across a change of leader. The producer id
is a Felix producer id, and the sequence numbers are written into the shard's
log with the records. Replicas get them along with the records, so when a
leader fails and a replica takes over, or an operator moves the shard, the new
leader already knows how far the producer got. A batch re-sent to it is
answered with the offset it was first written at and is not written again. A
batch whose start reached the new leader but whose end did not is finished, not
repeated.

```mermaid
sequenceDiagram
    participant P as Kafka producer
    participant A as Broker A (leader)
    participant B as Broker B (replica)
    P->>A: Produce records 40-44 (producer 7)
    A->>B: replicate records 40-44 with producer 7's sequences
    A--xP: answer lost, and A fails
    Note over B: B becomes leader, with 40-44 and their sequences
    P->>B: Metadata, then Produce records 40-44 again
    B-->>P: already written, at offset 1200
    P->>B: Produce records 45-49
    B-->>P: written at offset 1205
```

What it does not cover:

- **A producer that restarts.** A new process gets a new producer id and starts
  counting again, so anything it re-sends from its own buffer is new to the
  broker. That is the same in Kafka.
- **Very old re-sends.** Where each batch landed is remembered for the last 64
  batches per producer and shard. A re-send older than that is answered with
  `DUPLICATE_SEQUENCE_NUMBER`, which librdkafka and the Java client treat as
  delivered; the record is not written again, but the answer has no offset.
- **Producers that go quiet.** A shard remembers its 4096 most recently active
  producers, and a producer is also forgotten once retention has removed all of
  its records. A forgotten producer's next batch gets `UNKNOWN_PRODUCER_ID`,
  and the client takes a new id and carries on.
- **Epochs.** The epoch is always 0. When a producer would bump its epoch, it
  gets a new producer id instead, which comes to the same thing.

Each record written by an idempotent producer carries its producer id and
sequence in the log, which costs 20 bytes a record.

Sequences wrap from 2^31 - 1 to 0 as Kafka's do, and a batch may straddle the
wrap. Re-sends and gaps are recognised across it the same as anywhere else.

### acks and what they wait for

| `acks` | The answer means | On a `Leader` stream | On a `Quorum` stream |
|---|---|---|---|
| `0` | Nothing; there is no answer. The records are still written. | Written on the leader | Written on the leader |
| `1` | The leader has written it, as durably as the stream's fsync policy says | Same | The leader alone; replicas may not have it yet |
| `all` (`-1`) | The stream's own guarantee has been met | The leader has it, and that is all a `Leader` stream promises. `acks=all` does not wait for replicas here | A majority of the replica set has it |

`acks=all` never claims more than the stream gives. On a `Leader` stream it is
the same as `acks=1`, because a `Leader` stream acknowledges on the leader's
write by design. If you need a write to survive the leader, use a `Quorum`
stream and `acks=all`. When a `Quorum` write does not reach a majority in time,
the producer gets `REQUEST_TIMED_OUT` and retries; with idempotence on, the
retry cannot write the batch twice.

A producer only writes to the partition's leader. Sent anywhere else, a produce
gets `NOT_LEADER_OR_FOLLOWER` and the client finds the leader through Metadata,
exactly as it does for fetches.

### Transactions are refused

Felix has no transaction coordinator. A producer with `transactional.id` set
fails at its first step, looking up the coordinator, with an error that names
the reason. kcat 1.7.1 prints:

```console
% Using transactional producer
% ERROR: init_transactions(): Failed to find transaction coordinator: sasl_plaintext://broker-1.internal:9092/1480824465: Broker: Transactional Id authorization failed: Felix does not support Kafka transactions; unset transactional.id. See docs/kafka-compatibility.md
```

and exits. Unset `transactional.id`; an idempotent producer gives you
exactly-once writes per partition, which is often what the transaction was
there for.

## How it maps

**Partitions are shards.** A topic has one partition per shard, and partition N
is shard N.

**Offsets are Felix's.** The offset kcat prints is the offset in the shard's
log, the same one a Felix subscriber sees on `Event.offset`. They start at 0.
A consumer can see a gap of one where a new leader wrote its generation-start
record, which takes an offset but is never returned. Kafka allows such gaps, as
it does for its own control records. The earliest offset is the oldest record
retention has kept. The latest is the log's tail, or on a `Quorum` shard the
commit point if that is lower. Asking for an offset outside that range gets
`OFFSET_OUT_OF_RANGE`, and the client falls back to its `auto.offset.reset`.

**Records.** The value is the Felix payload. The timestamp is when the broker
appended the record. There is no key and there are no headers, whether the
record was written by a Felix client or a Kafka producer.

**Tenants come from the credential.** The SASL username is the tenant, and the
token must belong to it. Two tenants can each have an `orders.created` topic;
which one you read depends on who you are.

**Topics that do not appear.** In-memory streams, streams in a namespace that
contains a dot (the name would read back as a different stream), names with
characters outside `A-Z a-z 0-9 . _ -`, names longer than 249 characters, and
streams your token cannot read. Producing to any of them is refused the same
way reading is.

## When a leader moves

Each partition is served by its shard's leader, and Metadata tells the client
which broker that is. When a shard moves to another broker, or its leader fails
and a replica takes over, the old broker answers the next fetch with "not
leader". librdkafka asks for Metadata again and continues at the new leader
from the same offset. The new leader holds the same log with the same offsets,
so nothing is skipped or read twice. The consumer sees a short pause. A
producer does the same: its next produce to the old broker gets "not leader",
and it re-sends to the new one.

```mermaid
sequenceDiagram
    participant C as kcat
    participant A as Broker A
    participant B as Broker B
    C->>A: Fetch partition 1 from offset 58
    Note over A,B: shard 1 moves from A to B
    A-->>C: NOT_LEADER_OR_FOLLOWER
    C->>B: Metadata for orders.created
    B-->>C: partition 1 is led by broker B
    C->>B: Fetch partition 1 from offset 58
    B-->>C: records 58 onward
```

Every broker in a cluster needs the listener for this to work. A partition whose
leader has no Kafka listener is reported as having no leader.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `FELIX_KAFKA_LISTEN` | unset | `ip:port` to listen on. Unset turns the listener off. |
| `FELIX_KAFKA_ADVERTISE_ADDR` | the listen address | `host:port` clients are told to connect to. A hostname is fine. Every broker learns it through the control plane. |
| `FELIX_KAFKA_TLS` | `true` | TLS with the broker's certificate (`SASL_SSL`). `false` means `SASL_PLAINTEXT`, with tokens in clear text. |
| `FELIX_KAFKA_ANONYMOUS_TENANT` | unset | Development only: unauthenticated connections read and write every stream of this tenant. |
| `FELIX_KAFKA_DEFAULT_NAMESPACE` | unset | Namespace used for a topic name without a dot, so `created` can mean `orders.created`. |
| `FELIX_KAFKA_MAX_CONNECTIONS` | `1024` | Connections served at once. Extra ones are closed as they arrive. |
| `FELIX_KAFKA_MAX_CONNECTIONS_PER_IP` | `128` | Connections one source IP may hold, checked before the total. `0` is unlimited. |
| `FELIX_KAFKA_AUTH_TIMEOUT_MS` | `10000` | How long a connection has to finish SASL before it is closed. Until then each request is capped at 64 KiB. |

The listener's metrics are on the [Observability](/felix/features/observability/)
page, under `felix_kafka_*`.

## Troubleshooting

The messages below are as librdkafka prints them; other clients word them
differently but report the same codes.

**Unknown topic or partition.** The name does not resolve to a durable stream
you can see. Check that the stream was created with `durable: true`, that the
topic is `<namespace>.<stream>` (the split is on the first dot, so a namespace
with a dot in it cannot be reached), and that a topic without a dot has
`FELIX_KAFKA_DEFAULT_NAMESPACE` to fall back on. A partition number at or above
the stream's shard count gets the same error.

**Topic authorization failed.** Either the connection is not authenticated (no
SASL settings, and no anonymous tenant on the broker) or the token lacks the
permission: `stream.subscribe` to read, `stream.publish` to write. A stream you
cannot read also does not appear in `kcat -L`, but one you can read and not
write does, so a producer can see the topic and still be refused.

**SASL authentication failed.** The broker rejected the token, and the message
says why: `Felix: token rejected: ...`. The usual causes are a username that is
not the tenant the token was issued for, or an expired token. Tokens are checked
once, when the connection authenticates, so a long-running consumer is not cut
off when its token expires; the next connection needs a fresh one.

**Leader not available.** The shard is unassigned, still opening, or in the
middle of a move, or its leader is a broker without the Kafka listener. The
first three clear up on their own. The last needs `FELIX_KAFKA_LISTEN` on that
broker.

**Not leader for partition.** The shard moved or failed over, or the client
sent a produce to a broker that does not lead the partition. It is transient;
the client refreshes Metadata and carries on.

**Offset out of range.** The offset is older than anything retention kept, or
past the end of the log. The client resets according to `auto.offset.reset`.

**Waiting for group rebalance, then a consumer error.** The consumer used a
group (`kcat -G`, or `group.id` with `subscribe()`). Felix refuses it:

```console
% Waiting for group rebalance
% ERROR: Consumer error: FindCoordinator response error: Felix has no Kafka consumer groups; assign partitions instead. See docs/kafka-compatibility.md
```

Assign partitions with `-t topic -p N` (or `assign()`) instead.

**Transactional Id authorization failed.** The producer has `transactional.id`
set. Felix has no transactions; unset it.

**Unsupported for message format.** The producer wrote a v0 or v1 message set.
Every client from Kafka 0.11 on writes v2 record batches; set an old client's
`api.version.request=true` (librdkafka) or upgrade it.

**Out of order sequence number.** An idempotent producer sent a batch whose
sequence skips ahead of what the partition's log holds: something it believes
was written is not. librdkafka treats this as fatal for the producer. It should
not happen in normal operation; if it does, the stream's log was cut short
(a lost volume, say) and the producer's view is ahead of it.

**Unknown producer id.** The partition's log holds nothing from this producer:
it was idle long enough for retention to remove its records, or more than 4096
other producers wrote since. The client takes a new producer id and carries on
without losing anything.

**Message size too large.** A batch decompresses to more than 16 MiB. Lower the
producer's `batch.size` or `linger.ms`.

**Records arrive without keys or headers.** Expected: Felix records have
neither. See [Feeding Felix from Kafka producers you already
have](#feeding-felix-from-kafka-producers-you-already-have).

**SSL handshake failed.** Either the client does not trust the broker's
certificate (point `ssl.ca.location` at the CA that issued `FELIX_TLS_CERT`,
or at the file `FELIX_TLS_CERT_EXPORT` wrote), it is checking the hostname
against the generated certificate named `localhost` (set
`ssl.endpoint.identification.algorithm=none` on librdkafka 2.0 and later), or
the broker has `FELIX_TLS_CLIENT_CA` set and the client presented no
certificate from it.

**SASL fails with "the client certificate is not issued to the token's
subject".** The broker has `FELIX_TLS_CLIENT_CERT_BIND_SUBJECT=true`, and the
certificate the client presented names neither `felix:principal:<sub>` (a URI
SAN) nor a DNS or IP name matching the token's `sub`. Issue the client's
certificate to the principal its token is for.

**Connection closed right after connecting.** The client and broker disagree
about TLS: a `SASL_SSL` client against a broker with `FELIX_KAFKA_TLS=false`, or
a `SASL_PLAINTEXT` client against the default TLS listener. It can also be a
limit, and `felix_kafka_refused_total` says which: `reason="connection_limit"`
for `FELIX_KAFKA_MAX_CONNECTIONS`, `per_ip_limit` for
`FELIX_KAFKA_MAX_CONNECTIONS_PER_IP`, `auth_timeout` for a connection that did
not finish SASL within `FELIX_KAFKA_AUTH_TIMEOUT_MS`, and
`unauthenticated_frame_size` for a request over 64 KiB before authentication.

## Limits

Everything below is on purpose or not done yet. If your setup depends on one of
them, this is not the tool for that part of it.

**Refused, with an error that says why**

- Consumer groups: `subscribe()` with a `group.id`, committed offsets, and
  everything built on them (Kafka Connect, Kafka Streams, ksqlDB, Debezium,
  MirrorMaker). Use `assign()` and keep offsets yourself, or use Felix's own
  consumer groups through a Felix client.
- Transactions: any producer with a `transactional.id`.
- Old message formats (v0/v1 message sets). Every current client sends v2.

**Accepted, but not kept**

- Record keys. The key still picks the partition on the producer's side, but it
  is not stored, so a consumer reads every record with a null key. Anything that
  reads meaning from the key after that point will not see it.
- Record headers, dropped the same way.
- Producer timestamps. A record's timestamp is the broker's append time.
- Tombstones. A null value is stored as an empty payload, and since Felix does
  not compact, it deletes nothing.

Dropped keys and headers are counted in `felix_kafka_produce_dropped_total`, so
you can see whether a producer relies on them.

**Not there at all**

- Admin APIs. Topics cannot be created, deleted, reconfigured or given more
  partitions from Kafka tools (`kafka-topics.sh`, `AdminClient`, auto-create).
  A topic is a Felix durable stream: create it through the control plane, and
  its partition count is its shard count.
- In-memory streams. Only durable streams appear as topics.
- Namespaces with a dot in their name. The topic name is split on the first
  dot, so such a namespace cannot be reached and its streams are not listed.
- Log compaction and Kafka retention settings. Retention is the stream's own.
- Fetch sessions and leader epochs. Clients fall back to full fetch requests,
  and leader epochs are reported as unknown. The ISR is the leader alone.
- SASL mechanisms other than PLAIN: no OAUTHBEARER, no SCRAM, and no
  re-authentication on a long-lived connection.

**Behaves differently from Kafka**

- `acks=all` on a `Leader` stream waits for the leader only, because that is
  all a `Leader` stream promises. Use a `Quorum` stream when a write has to
  survive losing the leader. See [acks and what they wait
  for](#acks-and-what-they-wait-for).
- An idempotent re-send older than the last 64 batches a producer sent to a
  partition is answered `DUPLICATE_SEQUENCE_NUMBER` without an offset. It is not
  written twice, but the client does not learn where it landed.
- Produces count against the tenant's publish quota (`FELIX_TENANT_PUBLISH_*`).
  Over quota, the produce is still written, and the response carries
  `throttle_time_ms` instead of an error.
- A connection's produces are answered one at a time. For more throughput,
  spread across partitions, whose leaders are often different brokers.

**Size and time caps**

- A batch may decompress to at most 16 MiB.
- A request larger than 8 MiB closes the connection, and so does one larger
  than 64 KiB before the connection authenticates.
- A fetch waits at most 30 seconds, whatever `fetch.wait.max.ms` asks for.

**Tested clients**

kcat 1.7.1 (librdkafka 1.8.2) is tested for everything on this page. Other
librdkafka-based clients use the same code. The Java client speaks the same
protocol but has not been tested against Felix.
