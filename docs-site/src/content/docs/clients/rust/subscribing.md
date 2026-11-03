---
title: "Rust: Subscribing"
description: "Read a stream live or from a past offset or time, follow shards through moves, and resume after a restart."
---

Part of the [Rust client guide](/clients/rust/). Setup and configuration are on that page.

## Subscribing

### Creating Subscriptions

```rust
let mut subscription = client.subscribe("acme", "prod", "events").await?;

while let Some(event) = subscription.next_event().await? {
    process_event(event).await?;
}
```

`None` means the broker ended the stream. A lost connection is an error,
`SubscriptionLost`, so the loop above stops with `?` rather than exiting as if
the stream had finished. A `Client` subscription resubscribes by itself
from the offset after the last one it delivered; on an in-memory stream there
is no offset to resume from, so it returns the error. `next_event` is
cancel-safe, so wrapping it in `tokio::time::timeout` is fine: a resubscribe in
progress keeps running in the background and the next call picks it up.

### Event Structure

```rust
use bytes::Bytes;
use felix_client::RecordHeaders;
use std::sync::Arc;

pub struct Event {
    pub tenant_id: Arc<str>,
    pub namespace: Arc<str>,
    pub stream: Arc<str>,
    pub payload: Bytes,
    /// The log offset on a durable stream. `None` on an in-memory one, and
    /// against a broker that did not negotiate offsets.
    pub offset: Option<u64>,
    /// Offsets just before `offset` that hold no event. Non-zero only on the
    /// first event after a leader change.
    pub skipped_before: u64,
    /// Who published it, when `ClientConfig::publishers` asked and the broker
    /// recorded one.
    pub publisher: Option<Arc<str>>,
    /// When the broker appended it, in microseconds since the Unix epoch,
    /// when `ClientConfig::timestamps` asked. `None` on an in-memory stream.
    pub timestamp_micros: Option<u64>,
    /// The key and headers it was published with. `None` for a record that
    /// had neither, and against a broker that did not accept record headers.
    pub headers: Option<RecordHeaders>,
}
```

### Who published an event

Set `publishers: true` in the `ClientConfig` and each event says who published
it: `event.publisher` is the subject of the token the broker accepted the
write from. Group records carry the same value as `publisher`. Off by default,
since it adds the principal to every batch.

```rust
let mut config = ClientConfig::from_env_or_yaml(quic_client_config(None, false)?, None)?;
config.publishers = true;
let client = Arc::new(Client::connect(&seeds, "localhost", config).await?);
let mut inputs = client.subscribe("arena", "match-7", "inputs").await?;
while let Some(event) = inputs.next_event().await? {
    let Some(player) = event.publisher.as_deref() else {
        continue; // written before the broker recorded publishers
    };
    apply_input(player, &event.payload);
}
```

That is the broker vouching that the write came from that principal; it says
nothing about the payload's contents. An in-memory stream always reports it. A
durable stream reports it only for records written once the broker stores
publishers (the `publisher_principal` fleet feature, or
`FELIX_RECORD_PUBLISHERS=true` on a single broker), and then on replay and to
consumer groups as well.

### Sizing the broker's queue for a subscription

Each subscription has a bounded queue on the broker. Set
`broker_sub_queue_capacity` in the `ClientConfig` to ask for a different size
than the stream's default, counted in published batches. The broker clamps it
to its `FELIX_SUBSCRIBER_QUEUE_CAPACITY_MAX` and the subscription reports what
it got:

```rust
config.broker_sub_queue_capacity = Some(4096);
let client = Arc::new(Client::connect(&seeds, "localhost", config).await?);
let subscription = client.subscribe("acme", "prod", "webhooks").await?;
// `None` from a broker that predates the option: the stream's default applies.
let granted = subscription.queue_capacity();
tracing::info!(?granted, "subscription queue");
```

It applies to every subscription the client opens, including the ones a
`Client` reopens after a move or a lag. A larger queue makes drops
rarer; it does not change what happens on a drop, which the stream's policy
decides (see [When a subscription falls behind](#when-a-subscription-falls-behind)).

### When an event was written, and replaying from a time

Set `timestamps: true` in the `ClientConfig` and each event from a durable
stream carries `event.timestamp_micros`: when the broker appended it, in
microseconds since the Unix epoch. Group records carry the same value as
`timestamp_micros`. Off by default, since it adds eight bytes to every event.

To replay from a time, look up the offset and subscribe there.
`offset_for_time` works whether or not `timestamps` is set:

```rust
let start = client
    .offset_for_time("acme", "prod", "webhooks", 0, since_micros)
    .await?;
let position = match start {
    Some(offset) => StartPosition::Offset(offset),
    // Nothing that recent yet.
    None => StartPosition::Latest,
};
let replay = client
    .subscribe_from("acme", "prod", "webhooks", Some(position))
    .await?;
```

The answer is the first record on that shard appended at or after the time,
and the oldest record when the time is older than all of them. Times come from
the leading broker's clock, so the search is exact while that clock only moves
forward, and close otherwise. `client.supports_offset_for_time()` says whether
the broker answers it. `Client::offset_for_time` asks whichever broker
leads the shard, following redirects. The [Python](/clients/python/#replay-from-an-offset-or-a-time) and
[TypeScript](/clients/typescript/#replay-from-an-offset-or-a-time) bindings wrap it.

### Reading a range and stopping

`subscribe_from` keeps going into live delivery. To load a fixed slice, such as
a match's events up to a snapshot offset, read it page by page instead. No
subscription is opened:

```rust
let mut from = start;
while from < end {
    let page = client
        .read("acme", "prod", "matches", 0, from, Some(end), 500)
        .await?;
    if page.records.is_empty() {
        break; // nothing committed past here yet
    }
    for record in &page.records {
        apply(record.offset, &record.payload);
    }
    from = page.next_offset;
}
```

Continue from `next_offset` rather than the last offset plus one, since offsets
that hold no record are passed over. A page holds at most `max_records` (`0`
for the broker's cap) and is capped in size by the broker. A start below what
retention kept, or past the tail, fails with `SubscribeCursorError`.
`Client::read` does the same against whichever broker leads the shard,
and `client.supports_read()` says whether the broker answers it.

### Offsets are how you notice a drop

Subscriber queues shed under the default policy rather than blocking the
publisher, so a subscriber can silently miss records. **A jump between
consecutive offsets is a drop**, with one exception the broker tells you about:
a leader that takes or reopens the shard writes a generation-start record that
takes an offset and is never delivered, and the event after it says so in `skipped_before`.

```rust
let mut expected: Option<u64> = None;
while let Some(event) = subscription.next_event().await? {
    if let (Some(want), Some(got)) = (expected, event.offset) {
        // The first offset this event accounts for, past any that hold no event.
        let first = got - event.skipped_before;
        if first > want {
            tracing::warn!(dropped = first - want, "subscriber queue overflowed");
        }
    }
    expected = event.offset.map(|offset| offset + 1);
    handle(&event.payload);
}
```

Worth writing even if you never resume from offsets. It is the only signal the
queue overflowed. A broker older than the skip count reports none, so against
one a leader change reads as a drop of one.

### A consumer that survives a restart

Checkpoint what you handled and resume at the next one. `start` is the first
record you have **not** seen, so a resuming consumer passes `offset + 1`.

```rust
use felix_client::{StartPosition, SubscribeCursorError};

let mut start = match checkpoint.load().await? {
    Some(offset) => StartPosition::Offset(offset + 1),
    None => StartPosition::Earliest,
};

loop {
    let mut subscription = client
        .subscribe_from("acme", "prod", "events", Some(start))
        .await?;

    loop {
        match subscription.next_event().await {
            Ok(Some(event)) => {
                handle(&event.payload).await?;
                if let Some(offset) = event.offset {
                    checkpoint.save(offset).await?;
                    start = StartPosition::Offset(offset + 1);
                }
            }
            // Nothing left to read.
            Ok(None) if subscription.stream_deleted() => return Ok(()),
            // Ended for another reason: resubscribe from `start`.
            Ok(None) => break,
            Err(err) => {
                if let Some(cursor) = err.downcast_ref::<SubscribeCursorError>() {
                    // Retention discarded it. `available` is the nearest offset
                    // that would have worked, so this takes the smallest gap
                    // rather than restarting at the beginning, and says so,
                    // because a silent restart at the tail loses records with
                    // nothing reported.
                    tracing::error!(
                        requested = cursor.requested,
                        resuming_at = cursor.available,
                        "checkpoint is past retention",
                    );
                    start = StartPosition::Offset(cursor.available);
                    break;
                }
                return Err(err);
            }
        }
    }
}
```

`next_event` is **cancel-safe**: it awaits an `mpsc` receive, so racing it in a
`tokio::select!` consumes nothing when another branch wins. You can put a
timeout around it without losing a record.

### Where a subscription joined

On a durable stream, a subscribe with a start position tells you where it
joined:

```rust
let subscription = client
    .subscribe_from("acme", "prod", "orders", Some(StartPosition::Offset(1_000)))
    .await?;
let first = subscription.start_offset(); // Some(1000)
let live = subscription.live_offset(); // the tail when you subscribed
```

Events below `live_offset()` are catch-up; events from it on are new, and none
are skipped between the two. A reader that has received an event at
`live_offset() - 1`, or asked to start at `live_offset()`, has caught up: the
offset leaves out any generation-start records at the end of the log, which
never arrive as events. From `Latest` the two offsets are equal and both stop
short of those records, so reading history up to `live_offset() - 1` after a
`Latest` subscribe ends on an event that exists. Both are
`None` for a plain tail subscribe, an in-memory stream, or an older broker.

### When a shard moves

A rebalance or a drain can move a stream's shard to another broker. The old
owner delivers everything it committed, then ends the subscription with a
`shard_moved` frame saying where the shard went and where to resume.

`Client::subscribe` and `subscribe_from` (shard 0), and
`subscribe_shard` (a shard you name), return a `ClusterSubscription` that
follows the shard on its own: `next_event` resubscribes on the new owner
and carries on. On a durable stream it resumes at
`max(last delivered offset + 1, resume_from)`, so nothing is repeated or
skipped; an in-memory stream resumes at the new owner's tail. `moves()` counts
how often it followed, and `client()` is the connection it is on now.

A `Connection` subscription ends instead, and says why:

```rust
while let Some(event) = subscription.next_event().await? {
    handle(&event);
}
if let Some(moved) = subscription.shard_moved() {
    // Resubscribe at the larger of `moved.resume_from` and your last offset
    // + 1, on `moved.addr` if it is set, or via any broker, which redirects.
} else if subscription.stream_deleted() {
    // The stream was deleted: there is nothing to resubscribe to.
}
```

A `ClusterSubscription` does not follow a deleted stream: `next_event` returns
`None` and `stream_deleted()` says why.

See [Multi-node client](https://github.com/GetFelix/felix/blob/main/docs/multi-node-client.md#when-a-shard-moves).

### When a subscription falls behind

Under `DropNew` or `DropOld`, a subscriber that cannot keep up loses records,
in the broker's queue for it or in the client's own. On a durable stream the
subscription then ends: `next_event` returns the events queued before the drop
and then a `SubscriptionLagged` error whose `resume_from` is where the
broker's queue first dropped. It comes as soon as the drop happens, not with
the next publish. Resubscribe after the last event you received, not at
`resume_from`: the broker can also drop a frame on its way out to a slow
subscriber without reporting it, and that frame can sit below `resume_from`.

```rust
use felix_client::{StartPosition, SubscriptionLagged};

let mut last = None;
let resume_at = loop {
    match subscription.next_event().await {
        Ok(Some(event)) => {
            last = event.offset;
            handle(&event);
        }
        Ok(None) => return Ok(()),
        Err(err) => match err.downcast_ref::<SubscriptionLagged>() {
            // Subscribe again after the last record; the log still has the rest.
            Some(lagged) => break last.map_or(lagged.resume_from, |last| last + 1),
            None => return Err(err),
        },
    }
};
let subscription = client
    .subscribe_from("acme", "prod", "events", Some(StartPosition::Offset(resume_at)))
    .await?;
```

`ClusterSubscription` does this itself, so a slow reader catches up from the
log instead of losing records. An in-memory stream has no offsets, so it keeps
dropping as before; `felix_client_sub_queue_dropped_total` and
`felix_client_sub_dispatch_dropped_total` count those drops.

### Multiple Subscriptions

Handle multiple streams concurrently:

```rust
let mut orders = client.subscribe("acme", "prod", "orders").await?;
let mut inventory = client.subscribe("acme", "prod", "inventory").await?;
let mut logs = client.subscribe("acme", "staging", "logs").await?;

// `next_event` is cancel-safe, so the branches that lose a round lose nothing.
// Stops when any of the three ends.
loop {
    tokio::select! {
        event = orders.next_event() => match event? {
            Some(event) => handle_order(event).await?,
            None => break,
        },
        event = inventory.next_event() => match event? {
            Some(event) => handle_inventory(event).await?,
            None => break,
        },
        event = logs.next_event() => match event? {
            Some(event) => handle_log(event).await?,
            None => break,
        },
    }
}
```

### Async Event Processing

Slow work inside the read loop holds up the next event, and a reader that
falls behind loses records (see [When a subscription falls
behind](#when-a-subscription-falls-behind)). Hand the work to a task over a
bounded channel instead. A task per event would also keep up, but with no
limit on how many run at once or on the memory they hold.

```rust
use tokio::sync::mpsc;

let (events, mut queued) = mpsc::channel(100);
let worker = tokio::spawn(async move {
    while let Some(event) = queued.recv().await {
        expensive_processing(event).await?;
    }
    anyhow::Ok(())
});

while let Some(event) = subscription.next_event().await? {
    if events.send(event).await.is_err() {
        break; // the worker stopped; its result below says why
    }
}
// Closing the channel lets the worker drain what is queued and finish.
drop(events);
worker.await??;
```

Once the channel is full, `send` waits, and the reader slows to the worker's
pace like any slow reader.

### Subscription Lifecycle

```rust
let mut subscription = client.subscribe("acme", "prod", "events").await?;

// Read up to 100 events, then stop.
for _ in 0..100 {
    let Some(event) = subscription.next_event().await? else {
        break;
    };
    process(event);
}

// Dropping the subscription closes it on the broker.
drop(subscription);
```
