---
title: "Queues and Consumer Groups"
description: "Distribute work across consumers with acknowledgements, redelivery, and dead letters."
---

A **consumer group** reads a stream as work rather than as a broadcast. Where
every subscriber to a stream sees every record, the members of a group divide
the records between them: one consumer holds a record at a time, and the record
is not finished until someone says so.

It is the same log underneath — see
[Projections](/felix/architecture/projections/) — read through a cursor the
group shares instead of a cursor per subscriber.

![A consumer group reading a shard's log. Records are claimed by consumers A and B and acknowledged one by one, and the group's cursor advances behind them. When offset 4 is acknowledged while offset 3 is still in flight, the cursor stops at 3 rather than skipping it; once offset 3's claim lapses, it is redelivered, and settling it lets the cursor jump past both.](/felix/diagrams/consumer-group.svg)

The moment worth watching is the one in the middle. Offset 4 is acknowledged
while offset 3 is still held, and **the cursor stops at 3 anyway** — it only ever
advances over a contiguous run of finished records. That is what makes the
cursor safe to restart from: everything below it is genuinely done, so a broker
that restarts redelivers offset 3 and nothing before it.

The alternative — moving the cursor to the highest acknowledged offset — would
be simpler and would silently drop offset 3 on the next restart.

## The loop

```rust
loop {
    // Wait up to five seconds for work rather than spinning.
    let batch = client
        .group_poll_wait("t1", "default", "jobs", 0, "workers", 16, Duration::from_secs(5))
        .await?;

    for record in batch {
        match handle(&record.payload).await {
            Ok(()) => {
                client.group_ack("t1", "default", "jobs", 0, "workers", record.offset).await?;
            }
            Err(_) => {
                // Hand it back now rather than waiting out the timeout.
                client.group_nack("t1", "default", "jobs", 0, "workers", record.offset).await?;
            }
        }
    }
}
```

An empty batch means nothing was available, not an error.

## What the broker guarantees

**A record is held by one consumer at a time.** While a claim stands, no other
poll receives that record.

**A claim expires.** A consumer that stops answering does not hold a record for
ever: after `FELIX_GROUP_VISIBILITY_TIMEOUT_MS` the record is owed again and
goes to whoever polls next. This is why the loop above must be able to see the
same record twice.

**The cursor moves only over a contiguous run of acknowledgements.** If you
finish offset 6 while 5 is still outstanding, the group's saved position stays
below 5. Acknowledging out of order is fine; the position simply waits.

**Owed records go out before new ones**, so a redelivery is not starved behind a
fast producer.

> `an_offset_in_flight_is_not_handed_out_again`,
> `a_lapsed_claim_is_handed_out_again`,
> `the_cursor_does_not_advance_over_a_gap`,
> `owed_records_go_out_before_new_ones`.

## Retries and giving up

Every delivery carries `attempts`, counting this one. `1` is a first attempt;
anything higher is a redelivery, so a consumer can behave differently on a
retry — log it, route it elsewhere, or give up early.

After `FELIX_GROUP_MAX_ATTEMPTS` deliveries the broker gives up on a record: the
offset is recorded as a **dead letter** and the group moves past it. Without
that bound, one record that always fails stops the queue at that offset for
ever.

```rust
let dead = client.group_dead_letters("t1", "default", "jobs", 0, "workers").await?;
for offset in dead {
    // The record is still in the log at this offset.
    // Fixed the bug? Put it back:
    client.group_redrive("t1", "default", "jobs", 0, "workers", offset).await?;
    // Genuinely unprocessable? Stop tracking it:
    // client.group_discard("t1", "default", "jobs", 0, "workers", offset).await?;
}
```

**A dead letter is a pointer, not a copy.** The record stays in the stream's log
at that offset, readable by an ordinary replay. Nothing is duplicated, and
nothing is moved somewhere you have to go and find.

A redrive resets the record's attempt count and makes it owed again. It does
**not** move the group's cursor backwards, so everything already finished stays
finished. The redrive is durable: it is written to the dead-letter log before
the broker answers, so a leader lost before the record is finished leaves the
next leader owing it. It stays owed until a consumer acknowledges it, or until
it fails its attempts again and goes back on the list.

Redrive and discard change what every consumer of the group sees, so they need
`group.manage` (or `stream.manage`) on the stream. Polling, acknowledging,
handing back and listing dead letters need `group.consume`, which
`stream.subscribe` also grants. Either can be granted on one group
(`group:{tenant}/{namespace}/{stream}/{group}`) to keep a principal to the
groups it runs. See [Security](/felix/features/security/).

> `a_record_is_given_up_on_after_the_attempt_bound`,
> `a_redriven_record_is_handed_out_again`,
> `a_redriven_record_gets_its_attempts_back`,
> `a_redriven_record_is_still_owed_after_a_restart`.

### Limits

- A consumer can only acknowledge or hand back an offset the group has handed
  out. Anything else, such as an offset at or past the tail, is refused with
  `invalid_request`: acknowledging a record that does not exist yet would skip
  it when it is written.
- A group on a shard has at most `FELIX_GROUP_MAX_IN_FLIGHT` records handed
  out and unsettled at once. A poll past it answers empty, and is counted in
  `felix_group_polls_capped_total`, until acknowledgements, hand-backs or
  lapsed claims free room; a poll waiting at the cap is woken when they do.
- One poll hands out at most 1,000 records and stops reading after about 4 MiB
  of payload, whatever `max_records` asks for. The rest stay owed for the next
  poll.
- A group untouched for ten minutes (or twice the visibility timeout, if that
  is longer) has its in-memory state dropped and rebuilt from disk on its next
  request. That redelivers anything it still had in flight and restarts attempt
  counts, the same as a leader change.

Group state survives a leader failover. The position and the dead-letter list
replicate beside the shard's records, so a promoted leader resumes where the
group had got to, lists the records that were set aside, serves a redrive, and
still owes any record an operator redrove before the failover.

## What a queue does not promise

**Order.** A shared cursor gives it up the moment two consumers hold adjacent
records, and a redelivery reorders regardless of how many consumers there are. A
queue preserves the order records are *handed out* in and says nothing about the
order they are finished in. If you need per-key ordering, use a stream with a
routing key so related records land on one shard.

**Exactly-once.** A record can arrive twice — after a claim lapses, after a
leader is lost, after an idle group is rebuilt, or after a redrive. Handlers must tolerate seeing the same
record again.

**A consumer per group member.** A group is bound to the shard you name, and
nothing assigns shards across a group's consumers. Running one consumer per
shard is the application's job today; there is no coordinator handing shards
out.

## Configuration

| Variable | Default | What it controls |
| --- | --- | --- |
| `FELIX_GROUP_VISIBILITY_TIMEOUT_MS` | `30000` | How long a claim stands before the record is owed again. Too short redelivers work still being done; too long leaves a dead consumer's records stuck. |
| `FELIX_GROUP_MAX_ATTEMPTS` | `5` | Deliveries before a record is dead-lettered. |
| `FELIX_GROUP_MAX_WAIT_MS` | `30000` | Cap on how long a polling client may ask the broker to wait. |
| `FELIX_GROUP_MAX_IN_FLIGHT` | `10000` | Most records one group may have handed out and unsettled on a shard. Keeps one consumer that never answers from claiming the whole backlog. |

Groups need durable storage: without `FELIX_DURABLE_STORAGE_DIR` the broker does
not advertise the feature at all, because a position lost on every restart would
redeliver everything each time.
