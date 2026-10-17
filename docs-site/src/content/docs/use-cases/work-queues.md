---
title: "Work Queues"
description: "Hand background jobs to a pool of workers with acknowledgements, redelivery, retry limits and dead letters, on the same log that carries your events."
---

A checkout service accepts an order and has to do six slow things afterward:
charge the card, reserve stock, send a receipt, notify the warehouse, update
the search index and post to the accounting system. None of them should block
the response, any of them can fail, and a fleet of workers should share the
load. This is a work queue: producers add jobs, a pool of consumers takes each
job exactly once while it is being worked, and a job whose worker dies comes
back.

:::caution[Status: partial]
Consumer groups work today, including acknowledgements, redelivery,
dead letters, failover and a client that reads every shard. The status page
marks them partial for one reason: a group is bound to one shard, and the
broker serves one shard's group at a time. `ClusterClient::group_sharded`
merges shards on the client, and nothing assigns shards to consumers. The
default visibility timeout and the attempt bound are broker settings, not
per stream or per group. Groups need durable storage.
:::

## How this is usually built

Most teams use a managed queue or a broker. Amazon SQS hands a message to one
consumer and hides it for the visibility timeout (30 seconds by default). The
consumer deletes it when done, and if it does not, the message reappears. A
redrive policy moves a message to a dead-letter queue after a set number of
receives. It is operated by AWS and scales without planning, which Felix
does not match. RabbitMQ delivers to a consumer up to a prefetch limit and
waits for an ack. A nack or a closed channel returns the message to the queue,
and a dead-letter exchange takes the ones that are rejected or expire. It also
routes: exchanges, bindings and topics let one publish fan out to differently
shaped queues. NATS JetStream offers work-queue streams, where a message is
removed once acknowledged, with pull consumers, an ack wait and a maximum
number of deliveries.

Kafka is often pressed into service as a queue because it is already there.
A consumer group divides partitions among its members, so parallelism stops at
the partition count, a slow record holds up the partition behind it, and there
is no per-record acknowledgement or retry: a consumer commits an offset, and a
record that must be retried is re-published to a retry topic by the
application. What Kafka gives in return is ordering within a partition and
a log that can be replayed.

## How it works on Felix

A queue is a durable stream read through a **consumer group** instead of by
subscription. Where every subscriber sees every record, the members of a
group divide the records: a worker polls, holds the records it receives
for a visibility timeout, and acknowledges each when it is done. The records
live in the same log as the stream, so enqueuing is an ordinary publish, and
the same stream can also feed subscribers or a replay.

A claim that is not acknowledged lapses after the visibility timeout
(`FELIX_GROUP_VISIBILITY_TIMEOUT_MS`, 30 seconds by default), and the record
goes to whichever worker polls next, with `attempts` one higher. A worker can
hand a record back at once with `nack`, optionally with a delay, extend a claim
on slow work, or dead-letter a record it knows will never succeed. After
`FELIX_GROUP_MAX_ATTEMPTS` deliveries (5 by default) the broker records the
offset as a dead letter and the group moves past it, so one poison job does
not stop the jobs behind it. A dead letter is a pointer to the record, not a
copy: the record stays in the log, and `group_redrive` makes it owed again
after the bug is fixed.

The group's position and its dead letters replicate with the shard, so a
promoted replica resumes where the group was. The position advances only over
a contiguous run of finished records, so a restart redelivers anything
unfinished and never skips it. Group writes are acknowledged once durable on
the leader, so a failover can lose the newest ack and redeliver that record.

Delivery is at-least-once. A crash between finishing the work and
acknowledging it looks like a crash before starting, and the job comes back.
`attempts` greater than 1 tells a worker it is looking at a retry.

On the producing side, the failure that matters is a lost acknowledgement that
makes the client send a job twice. An idempotent producer numbers its batches
and the shard's leader answers a re-send of a sequence it already holds
without appending it, so a retried enqueue lands once. That removes duplicates
caused by the publish path. It does not remove duplicates the application
makes, such as two requests for the same order.

## Example

Create a replicated durable stream marked as a queue, then a group at the
earliest record:

```bash
felixctl stream create jobs --kind queue --shards 4 --replication 3 --consistency quorum
felixctl group create jobs workers --from earliest
felixctl pub jobs '{"order":42,"task":"send-receipt"}'
felixctl group poll jobs workers --max 5
felixctl group ack jobs workers 0:15:1
felixctl group describe jobs workers
```

`poll` prints each record as `CLAIM<TAB>payload` where the claim is
`SHARD:OFFSET:ATTEMPTS`, and `ack` takes the claim. `describe` shows each
shard's lag, in-flight and owed counts and dead letters. See
[felixctl consumer groups](/getting-started/felixctl/#consumer-groups).

Enqueuing from Rust, with retries that cannot duplicate a job:

```rust
use std::sync::Arc;

let cluster = Arc::new(cluster);
let producer = cluster.idempotent_producer().await?;

// Jobs for one order share a shard, and so a sequence.
producer
    .publish_keyed("acme", "prod", "jobs", bytes::Bytes::from("order-42"), job.encode())
    .await?;
```

Working the queue across every shard. `group_sharded` polls the shards in
turn and routes each ack to the shard the record came from. Run as many
workers as you like on the same group name:

```rust
use std::time::Duration;

let group = cluster.group_sharded("acme", "prod", "jobs", "workers").await?;

loop {
    let batch = group.poll(16).await?;
    if batch.is_empty() {
        tokio::time::sleep(Duration::from_millis(200)).await;
        continue;
    }
    for item in batch {
        if item.record.attempts > 1 {
            tracing::warn!(offset = item.record.offset, "retry");
        }
        match handle(&item.record.payload).await {
            Ok(()) => group.ack(&item).await?,
            Err(_) => group.nack_after(&item, Duration::from_secs(30)).await?,
        }
    }
}
```

`ShardedGroup::poll` waits for no one, so the loop sleeps on an empty batch.
A single-shard consumer can long-poll instead with `group_poll_wait`, which
asks the broker to hold the request until work arrives or the wait elapses.

The Python binding has the same calls per shard (`group_poll`, `group_ack`,
`group_nack`, `group_dead_letters`, `group_redrive`, `group_discard`):

```python
for record in client.group_poll("acme", "prod", "jobs", 0, "workers", max_records=16, wait=5.0):
    try:
        handle(record.payload)
        client.group_ack("acme", "prod", "jobs", 0, "workers", record.offset)
    except Exception:
        client.group_nack("acme", "prod", "jobs", 0, "workers", record)
```

A worker that restarts and wants its in-flight jobs back immediately, instead
of after the visibility timeout, polls under a stable name with
`group_poll_as` and a `GroupMember` whose `reclaim` is set (`consumer=` and `reclaim=` in Python).

## Compared with SQS, RabbitMQ, Kafka and JetStream

SQS is the simplest to run if you are on AWS, and it scales without any
shard count to choose. Felix needs you to run brokers and pick a shard count
at creation. In exchange, a queue is a log: a job that failed is still
there to read, a dead letter is a pointer and not a second queue to drain,
and one stream can feed both a worker pool and an audit subscriber. SQS FIFO
queues order within a message group and deduplicate by deduplication ID. Felix
hands out new records in offset order, with redeliveries first, and promises
nothing about the order they finish.

RabbitMQ is the better choice when routing is the problem. Exchanges,
priorities and per-message TTL have no counterpart in Felix, which has a
stream and a group. Felix is the better fit when the queue shares a system
with streaming, state and replay and you want one thing to operate.

Compared with Kafka consumer groups, workers are not limited by partition
count. Any number of workers poll one group, each record goes to one of them,
and a slow record delays only its own worker. There is per-record
acknowledgement, retry delay and a dead-letter list. What you give up is
ordering across consumers, which Kafka preserves per partition. If jobs for
one key must run in order, keep them on one shard with a routing key and
accept that two workers can still hold adjacent records.

JetStream work-queue streams are the closest design: pull consumers, ack
wait, maximum deliveries. JetStream removes an acknowledged message from the
stream under work-queue retention, while a Felix record stays until retention
removes it, which keeps replay and redrive available but means storage is
bounded by retention and not by what is still pending.

## Where it stops

A group is bound to one shard and nothing assigns shards to workers, so a
worker must poll every shard itself or be told which ones to poll, which
`ShardedGroup` does for you. There are no priorities, no per-message TTL, no
delayed enqueue (a delay applies to a nack, not a first delivery), and no
routing beyond stream and key. The attempt bound and default visibility are
broker settings. A group untouched for ten minutes has its in-memory state
rebuilt from disk, which redelivers what it still had in flight. Order of
completion is not guaranteed, and a handler must tolerate seeing a job twice.
Python and Rust clients have the calls; the Python binding has no sharded
helper and no idempotent producer. A group needs `group.consume` to poll and
settle, and `group.manage` to redrive or discard, which a role can hold on a
single group.

## Related

- [Queues and consumer groups](/features/queues/)
- [Demo: queue semantics](/demos/queue-semantics/)
- [Rust consumer groups](/clients/rust/#consumer-groups)
- [Idempotent producers](/clients/rust/#idempotent-producers)
- [Atomic commits](/features/atomic-commits/), for writing a job and the state it belongs to as one record
