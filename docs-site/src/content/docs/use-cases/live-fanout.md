---
title: "Live Updates at High Fanout"
description: "Send one stream of updates to thousands of subscribers (dashboards, scoreboards, market-data-like feeds, notifications) without a slow subscriber slowing the publisher or anyone else."
---

A trading dashboard shows prices that change a few hundred times a second to
2,000 open sessions. A scoreboard service pushes every goal to everyone watching
a match. A notification system tells each logged-in user about a new comment.
Every subscriber wants the same recent updates quickly. Nobody wants one phone
on a bad connection to slow down the other 1,999 sessions or back up the
publisher, and when a subscriber does fall behind, it has to be able to tell.

## How this is usually built

For small payloads and low latency, teams use NATS core or Redis pub/sub. Both
are fire-and-forget: the server holds a bounded buffer per client and a client
that cannot keep up is cut off. NATS core disconnects a client whose pending
data exceeds its limit (a "slow consumer"), and a subscription can also be given
its own pending limits so that it drops messages and reports an error. Redis
disconnects a pub/sub client once its output buffer passes the
`client-output-buffer-limit` for the pubsub class. Both protect the server and the
other subscribers, which is the right instinct. What the subscriber gets
afterward is a closed connection and no record of what it missed, so a page that
mirrors state has to reconnect and refetch everything from somewhere else.

Kafka solves the missed-data problem the other way. Consumers pull from a log, so
a slow consumer just lags, and nothing is dropped until retention removes it.
Fanout is where it hurts. Each consumer group reads the log independently, so
2,000 independent dashboards mean 2,000 groups' worth of fetches, so broker work
grows with the number of groups. Teams then add a relay tier that consumes from Kafka once
and pushes to clients over WebSockets, which is another system to run and size.

## Why Felix fits

A publish is encoded once and every subscriber of the stream shares the encoded frame. Adding a subscriber adds one write to its connection and no encoding, which is the cost that dominates when 2,000 sessions want the same tick.

Each subscription has its own bounded queue and its own QUIC stream with its own flow-control window. With the default `drop_new`, a phone on a bad connection loses its own records and nobody else waits. The policy is a broker setting, so a subscriber cannot choose `block` and stall every publisher by reading slowly.

On a durable stream the subscriber can tell. Every event carries its log offset, and a queue overflow ends the subscription with `subscription_lagged` and the offset where the drop began. `ClusterClient` resubscribes there and replays the gap from the log. NATS core and Redis pub/sub close the connection and leave the page to refetch everything.

The push path reaches many readers of the same data without a per-reader fetch. On a real Azure cluster Felix delivered 1.0 million messages a second to 500 subscribers from one broker with zero loss, with the publisher's ack latency flat at about 206 microseconds (256 B payloads, one stream on one shard, paced publisher, batch 1, per-message ack; [conditions](/features/real-network-performance/#fanout)). That is delivered throughput, not the publish rate.

Kafka's answer to the same load is a relay tier that reads the log once and pushes to clients. Here the broker is that tier for QUIC clients, and [the gateway](/use-cases/browser-realtime/) is the WebSocket edge for browsers.

## How it works on Felix

A publish is encoded once and the encoded frame is shared by every subscriber of
the stream. Adding subscribers adds delivery work (a write to each subscriber's
connection) but no extra encoding. Delivery is batched per subscription, up to
64 events, 64 KiB or 250 microseconds under load, and a trickle goes out event
by event with no added delay.

Each subscription has its own bounded queue in the broker and its own QUIC
stream with its own flow-control window. The queue is bounded by entry count and
by bytes. When it is full, the broker applies the overflow policy
(`FELIX_SUB_QUEUE_POLICY`):

`drop_new`, the default, discards the newest record for that subscriber alone. The
publisher and the other subscribers never wait. `block` holds the publish until the
slow subscriber has room, so nothing is dropped and the whole stream moves at the
slowest subscriber's pace. `drop_old` is accepted but behaves as `drop_new` today.
A subscriber can ask for a larger queue when it subscribes, but cannot choose the
policy, since a subscriber that could choose `block` could stall every publisher
on the shard just by reading slowly.

On durable streams a drop is visible: every delivered event carries its log offset, so a jump in offsets is exactly a
drop. A durable subscription whose queue overflows is ended with
`subscription_lagged` and the offset where the drop began. `ClusterClient`
resubscribes there and replays the gap from the log, so a slow reader catches up
instead of losing data. A non-durable stream has no offsets, so a drop there is
counted in the broker's metrics and invisible to the subscriber. That is the
right trade for a value that the next update replaces, such as a price or a
cursor position, and the wrong one for anything the subscriber must act on.

| Stream | Offsets on events | A slow subscriber |
|---|---|---|
| Non-durable (`--durable false`) | none | Loses records for good, silently to itself |
| Durable | yes | Sees the gap, or is ended with `subscription_lagged` and resumes from the log |

## Example

Create a stream for ticks that are worthless once superseded, and a durable one
for events that must not be lost:

```bash
felixctl stream create ticker --durable false --delivery at-most-once
felixctl stream create alerts --retention-secs 86400
felixctl sub ticker
felixctl pub ticker '{"sym":"ACME","px":101.25}' --count 100
```

Publishing from Rust, with no ack for the lossy feed and a batch for throughput:

```rust
use felix_client::{AckMode, ClientConfig, ClusterClient};

let quinn = quinn::ClientConfig::with_platform_verifier();
let client = ClusterClient::connect(&["127.0.0.1:5000".parse()?], "localhost",
    ClientConfig::optimized_defaults(quinn)).await?;

client
    .publish("acme", "prod", "ticker", br#"{"sym":"ACME","px":101.25}"#.to_vec(), AckMode::None)
    .await?;

let batch: Vec<Vec<u8>> = (0..64).map(|i| format!("tick {i}").into_bytes()).collect();
client
    .publish_batch("acme", "prod", "alerts", batch, AckMode::PerBatch)
    .await?;

// Flush the unacknowledged ticker publish before exiting.
client.finish().await?;
```

A subscriber on a durable stream turns a jump in offsets into a count of missed
records. `skipped_before` accounts for the record a new leader writes when it
takes over, which takes an offset and is never delivered:

```rust
let mut subscription = client.subscribe("acme", "prod", "alerts").await?;
let mut expected: Option<u64> = None;
while let Some(event) = subscription.next_event().await? {
    if let (Some(want), Some(got)) = (expected, event.offset) {
        let start = got - event.skipped_before;
        if start != want {
            tracing::warn!(dropped = start - want, "subscriber queue overflowed");
        }
    }
    expected = event.offset.map(|offset| offset + 1);
    render(&event.payload);
}
```

A `Client` subscription that falls behind returns a `SubscriptionLagged` error
from `next_event`, with a `resume_from` that says where the drop began. Resubscribe
with `subscribe_from` after the last event you handled. A `ClusterClient`
subscription does this itself, so a slow reader catches up from the log.

In Python the same check is:

```python
expected = None
with client.subscribe("acme", "prod", "alerts") as events:
    for event in events:
        start = event.offset - event.skipped_before
        if expected is not None and start != expected:
            log.warning("dropped %d records", start - expected)
        expected = event.offset + 1
        render(event.payload)
```

The Python subscription resubscribes after the last record it delivered when a
durable stream's queue overflows, so the log supplies what was dropped.

To see isolation measured, run `task demo:slow-consumer`: it stalls one of three
subscribers and compares both policies. Under `drop_new` the publisher held its
target rate and the healthy subscribers lost nothing, and the stalled one lost
about 120,000 events permanently. Under `block` nothing was lost anywhere and
the publisher dropped from 20,000 to about 16,400 messages a second. That is a
single-process loopback run at fanout 3, in
[Demo: Slow-consumer Isolation](/demos/slow-consumer-isolation/).

## Compared with NATS core, Kafka and Redis pub/sub

NATS core has the lowest overhead of the three and a subject model with wildcards that Felix does not have. If your feed is fire-and-forget and a reconnect with a full refetch is cheap, NATS core is simpler and more mature. Felix differs in what a lagging subscriber gets: its connection stays up, its queue is bounded per subscription, and on a durable stream it learns exactly where it fell behind and reads the gap from the log. JetStream adds durability to NATS with pull and push consumers; a Felix durable stream with resumable subscriptions is the comparable piece.

Kafka is the better system when consumers may be minutes or hours behind, when you need replay of a long retained history, or when the work is stream processing. For pushing the same data to many readers, each Felix subscriber receives a frame the broker has already encoded once, while each Kafka consumer group re-reads the log, so broker work grows with the number of groups. In the same Azure rig Redpanda served about 912,000 messages a second to 8 consumer groups, but the Kafka-protocol client machines saturated first, so that figure is a floor for Redpanda and the comparison does not settle which scales further.

Redis pub/sub is the simplest to adopt and shares NATS core's trade: the output buffer limit protects the server by disconnecting the client, and the message is gone. If you already run Redis for caching it costs nothing extra. Felix keeps the subscriber connected and, on a durable stream, gives it the offset to resume from. Redis Streams add offsets and consumer groups.

At fanout 10 on an M4 Max over loopback, Felix measured 237 microseconds p50 and 327 microseconds p99 for a 256 B payload with a per-message ack ([benchmarks](/features/benchmarks/)). Those numbers are for a single host.

## Try it

Start the stack from the
[Quickstart](/getting-started/quickstart/#try-it-in-one-command) (it covers Podman
too) and run these from the directory holding `compose.yaml`.

Start a subscriber in each of two terminals:

```bash
docker compose run --rm felixctl sub orders
```

Then publish from a third:

```bash
docker compose run --rm felixctl pub orders --count 3 'tick'
```

Both subscribers print all three records. To see a subscriber that falls behind
while the others carry on, run the
[slow-consumer demo](/demos/slow-consumer-isolation/).

## Where it stops

Fanout above 10 has not been benchmarked on loopback, and the largest measured
fanout is 500 subscribers on a real network. A deliberately slow subscriber inside
a large healthy fleet has not been measured on one; the isolation numbers above are
from three subscribers on loopback. Treat thousands of subscribers per broker as
something to test with your own payload and queue sizes.

`drop_new` means a subscriber on a non-durable stream cannot know it missed
anything. If you need that knowledge, use a durable stream, which costs disk. If
you need every record handled exactly once by someone, use a consumer group
(see [Queues](/features/queues/)); a plain subscription is at-most-once.
Exactly-once delivery is not offered.

The overflow policy is a broker setting, not a per-subscriber choice. A
`block` policy lets a single stalled subscriber that stays connected hold every
publisher on its shard until it reads or disconnects, with no timeout.

Browsers cannot connect directly, since Felix clients speak QUIC. See
[Browsers and mobile clients](/use-cases/browser-realtime/).

## Related

- [Publish/subscribe](/features/pubsub/) for batching, queue sizing and the byte bounds
- [Demo: Slow-consumer Isolation](/demos/slow-consumer-isolation/) and [Demo: Local State Divergence](/demos/state-divergence/)
- [Benchmarks](/features/benchmarks/) and [Real-network performance](/features/real-network-performance/)
- [Config and feature flags](/use-cases/config-and-feature-flags/) for state that must converge rather than just arrive
