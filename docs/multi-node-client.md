# Running a client against a cluster

For application developers. What to configure, what the client does on its own,
and what it will not do for you.

`docs/client-config.md` covers the tuning knobs — pool sizes, windows, queue
policies. This page is about the cluster: how a client finds brokers, what
happens when one dies, and which failures it retries.

Every guarantee named here is defined in [`semantics.md`](semantics.md), which
is the contract; this page is how to reach it from an application.

## Use `ClusterClient`, not `Client`

`Client` holds connection pools to **one** broker. It is the right thing for a
single-node deployment and the wrong thing for a cluster: the broker it was
given is exactly the one that may fail over, and when it does, that client stops
working while every other broker sits there able to serve.

`ClusterClient` owns a `Client` and rebuilds it from the other endpoints when
the one in use fails.

```rust,no_run
use std::time::Duration;
use felix_client::{ClientConfig, ClusterClient, ReconnectPolicy};
use felix_wire::AckMode;

# async fn example(quinn: quinn::ClientConfig) -> anyhow::Result<()> {
let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
config.auth_tenant_id = Some("acme".to_string());
config.auth_token = Some(std::env::var("FELIX_TOKEN")?);
// For a long-running client, set `config.token_provider` so reconnects get a
// fresh token. See docs/auth.md.

// Every broker you know of. One is enough — see "Discovery" below.
let seeds = ["10.0.0.4:5000".parse()?, "10.0.0.5:5000".parse()?];

let client = ClusterClient::connect_with_policy(
    &seeds,
    "broker.internal",
    config,
    ReconnectPolicy {
        attempts: 5,
        backoff: Duration::from_millis(200),
        max_backoff: Duration::from_secs(2),
        // Off by default. See "Retries" for why there is no useful default.
        deadline: Some(Duration::from_secs(10)),
    },
)
.await?;

client
    .publish_at_least_once("acme", "default", "orders", b"payload".to_vec(), AckMode::PerMessage)
    .await?;
# Ok(())
# }
```

`ClusterClient::connect` uses the default policy, which is the same thing
without the `deadline`.

## Discovery

**One seed address is enough.** On connect, and after every reconnect, the
client asks the broker it reached which other brokers a client may use, and adds
them to what it will try.

- The addresses you configured are **never removed**. A wrong or stale answer
  therefore cannot leave the client with fewer ways in than you gave it, which
  is what makes it safe to take the answer at all.
- A broker is offered only when the cluster considers it able to serve *and* it
  has been told where clients reach it (`FELIX_CLIENT_ADVERTISE_ADDR` on the
  broker). One that has not is left out rather than handed over at an address
  that would refuse the connection.
- An empty answer is normal on a single-node deployment. It is not an error.

`ClusterClient::endpoints()` returns everywhere the client would try, seeds
included. `refresh_topology()` asks again on demand; you do not normally need
it, because reconnecting already does.

Still configure more than one seed if you can. Discovery needs *some* broker to
answer before it can tell you about the others.

## Failover

When the broker in use goes away:

- **`publish` reconnects and does not resend.** It returns the error with the
  connection already replaced, so your next call goes somewhere live. Use it
  when a duplicate is worse than a gap, and handle the error.
- **`publish_at_least_once` reconnects and resends.** It can therefore deliver
  the record twice — the broker cannot tell, because only your application holds
  an identity that would make deduplication possible. The name is the contract.

**`idempotent_producer` reconnects, resends, and does not duplicate.** The
producer takes an id from the broker and numbers its batches; a batch re-sent
under the same number is answered from what the shard already holds rather than
appended again, so the ambiguous publish above stops being ambiguous. A batch
for a shard led elsewhere is refused with the leader's address and the
producer follows it. On a durable stream that holds across a leader change:
the sequences are stored in the log and replicated with it, so the broker
leading next answers the batch in flight, whether it got there by failover or
by a planned move, and the producer carries on. What it does not cover is a
producer the shard has forgotten — retention removed all its batches — or an
in-memory stream's new leader: the broker says `unknown_producer`, and the
producer ends on that stream with a typed refusal rather than guessing. See
`docs/protocol.md`, "Idempotent producers".

**A single-shard subscription does not survive a failover.** It is bound to the
connection it was created on. Record `Event.offset` as you go and resubscribe
from `offset + 1`; that is what offsets are for, and it is the only way to
resume without a gap.

**A sharded subscription reconnects each shard on its own** — see below.

## When a shard moves

A rebalance or a drain moves a shard from one broker to another while both stay
up. That is not a failover: the old owner ends the shard's subscriptions and
cache watches itself, after delivering everything it committed, and its last
frame on each says where the shard went and where to resume (`shard_moved`, see
`docs/protocol.md`, "Shard moves").

**A `ClusterClient` subscription follows the shard.** `ClusterClient::subscribe`
and `subscribe_from` return a `ClusterSubscription`, and its `next_event`
resubscribes on the new owner by itself, so a move looks like a short pause:

```rust,no_run
# async fn example(cluster: std::sync::Arc<felix_client::ClusterClient>) -> anyhow::Result<()> {
let mut subscription = cluster.subscribe("t1", "default", "orders").await?;
while let Some(event) = subscription.next_event().await? {
    // Carries on across a move. `subscription.moves()` counts them.
}
# Ok(())
# }
```

- **On a durable stream the resume is exact.** It resumes at
  `max(last delivered offset + 1, resume_from)`, so nothing is repeated and
  nothing is skipped that the subscriber's own queue did not drop.
- **An in-memory stream resumes at the new owner's tail**, as any resubscribe
  would. Its offsets mean nothing on another broker.
- **It asks the broker the old owner named first**, and the entry broker (which
  redirects) if that one is unreachable. The old owner sends `shard_moved`
  before the new owner has taken over, so the first attempts may be refused;
  it retries with the reconnect policy's backoff until its `deadline`, or 30
  seconds without one, and then returns the error from `next_event`.

`subscribe_sharded` does the same per shard and reports it as
`ShardEvent::ShardMoved`.

**A `ClusterClient` cache watch follows too.** `watch_cache` and
`watch_cache_retained` return a `ClusterCacheWatch`. Its `recv` hands out
`CacheWatchItem::ShardMoved` as a notice and then reopens the watch on the new
owner from the larger of the old owner's `resume_from` and the offset after the
last change handed out. The cache log moves with the shard and keeps its
offsets, so no change is repeated or skipped; the one exception is a resume
point the new owner has already compacted past, where the watch resnapshots as
any resume from that offset would. A `Lagged` still ends the watch, and so does
not reaching the new owner by the deadline; `resume_from()` is then where to
start a new one. A sharded cache watch follows each shard the same way and
reports `ShardedCacheWatchItem::ShardMoved`, or `ShardClosed` if that shard
could not be followed.

**With `Client`, you resume.** The subscription ends: `next_event` returns
`None` with `Subscription::shard_moved()` set to the `ShardMoved` it received.
Resubscribe at the larger of `resume_from` and your last offset plus one. A
cache watch delivers `CacheWatchItem::ShardMoved` last; re-watch from its
`resume_from` when set, and otherwise from the offset after the last change you
saw.

## Consuming a whole multi-shard stream

A subscription reads **one shard**. A stream's shards can have different owners
and a subscription is bound to one connection, so reading a four-shard stream
means four subscriptions, each following its own redirect.

`ClusterClient::subscribe_sharded` does that for you:

```rust
let client = Arc::new(ClusterClient::connect(&seeds, "localhost", config).await?);
let mut subscription = client
    .subscribe_sharded("t1", "default", "orders", Some(StartPosition::Earliest))
    .await?;

while let Some(item) = subscription.next().await {
    match item {
        ShardEvent::Record { shard, event } => handle(shard, event),
        // Not an error and not silence: this shard is down and being
        // re-established, and the others are still delivering.
        ShardEvent::ShardLost { shard, error } => warn!(shard, %error, "shard down"),
        ShardEvent::ShardRecovered { shard } => info!(shard, "shard back"),
        // Being followed to its new owner; records carry on from there.
        ShardEvent::ShardMoved { shard, moved } => info!(shard, ?moved, "shard moved"),
        // `ShardEvent` is non-exhaustive.
        _ => {}
    }
}
```

Five things about it are deliberate, and each is a choice you would otherwise
have to make yourself:

- **The merge happens in the client, and stays there.** The broker serves
  shards, not merged streams. A stream's shards live on different brokers, so a
  broker-side merge would pull every other shard's records to one broker, send
  each record twice, and put a stream's whole read load on one broker. Spreading
  that load is the reason for sharding. The merged order it could offer would be
  invented, since no order exists across shards, and resuming would still need
  the per-shard offsets below. Each shard's fanout is already shared by every
  subscriber to it, so merging in the client costs the broker nothing. The
  Python and TypeScript clients wrap this same implementation
  (`subscribe_sharded` / `subscribeSharded`). A client without a Felix library
  does the same thing on the wire: `StreamShards` for the count, then one
  `Subscribe` per shard.

- **Ordering is per shard, and nothing more.** Two records from one shard arrive
  in the order they were written. Two records from different shards arrive in an
  arbitrary order. Merging cannot restore an order that never existed — Felix
  orders per key, and a key always resolves to one shard. Do not infer
  stream-wide ordering from the fact that these arrive on one channel.

- **Resumption is a vector, not a number.** `Event.offset` is per shard, so one
  number cannot say where a sharded consumer got to.
  `ShardedSubscription::positions` hands back one offset per shard; pass it to
  `resubscribe_sharded` to carry on. Each listed shard resumes at `offset + 1`.

- **An unreachable shard refuses the whole subscription.** Opening covers every
  shard or it fails, naming the shards it could not reach. A subscription
  quietly covering three shards of four looks exactly like a complete one to
  everything downstream, which makes it the worst available answer.

- **Losing one shard's owner does not tear down the others.** That shard is
  re-established on its own, resuming after the last offset it delivered, and
  you are told with `ShardLost` and `ShardRecovered` rather than left to infer
  it from a gap.

It needs a broker advertising `FEATURE_STREAM_SHARDS`, because the shard count
comes from asking one. A broker that has never heard of the stream reports zero
shards and the call fails rather than reading shard 0 and calling it the stream.

## A prefix watch across every shard of a cache

Keys sharing a prefix hash to different shards, and a cache watch reads one.
`ClusterClient::watch_cache_sharded` opens one prefix watch per shard, follows
each shard's redirect to its owner, and merges them:

```rust,no_run
# use felix_client::ShardedCacheWatchItem;
# async fn example(cluster: std::sync::Arc<felix_client::ClusterClient>) -> anyhow::Result<()> {
let mut watch = cluster
    .watch_cache_sharded_retained("t1", "default", "sessions", "user:")
    .await?;
while let Some(item) = watch.recv().await {
    match item {
        ShardedCacheWatchItem::Change { shard, change } => { /* apply */ }
        ShardedCacheWatchItem::StateComplete => { /* every shard's state is in */ }
        ShardedCacheWatchItem::Lagged { shard, .. } => { /* that shard ended */ }
        ShardedCacheWatchItem::ShardMoved { shard, .. } => { /* followed to its new owner */ }
        ShardedCacheWatchItem::ShardClosed { shard } => { /* that shard ended */ }
    }
}
# Ok(())
# }
```

- **Ordering is per key.** A key's changes arrive in write order. Changes to
  keys on different shards can arrive in any order, and each `offset` belongs
  to its own shard's log.
- **`StateComplete` arrives once**, after every shard has sent its retained
  values. Shards finish at different times, so you can't work this out by
  counting. If a shard ends partway through, it never arrives.
- **Resume per shard.** `resume_offsets()` has one offset per shard; pass it
  back to `watch_cache_sharded`. A shard that hadn't finished its retained
  values resumes at 0.
- **A shard that ends is reported, not reconnected.** You get `Lagged`,
  `ShardMoved` or `ShardClosed` for it and the other shards carry on. After
  `Lagged` or `ShardMoved`, `resume_offsets()` already says where that shard
  resumes.
- **If any shard can't be reached, the call fails**, as with
  `subscribe_sharded`.

It needs a broker advertising `FEATURE_CACHE_SHARDS` to learn the shard count.

## A consumer group across every shard

A group is bound to one shard, and only that shard's leader can serve it: the
group's cursor and its claims live there. `ClusterClient::group_sharded` keeps
one group per shard and follows each shard's redirect to its leader:

```rust,no_run
# async fn example(cluster: std::sync::Arc<felix_client::ClusterClient>) -> anyhow::Result<()> {
let group = cluster.group_sharded("t1", "default", "jobs", "workers").await?;
loop {
    for claimed in group.poll(32).await? {
        // `claimed.shard` travels with the record, so the ack goes to the
        // broker that holds that shard's group.
        group.ack(&claimed).await?;
    }
}
# }
```

- **Each poll takes one shard's batch, visiting shards in turn.** Every shard
  is reached and none can starve the rest; an empty answer means no shard had
  anything.
- **Ordering is per shard.** As with `subscribe_sharded`, records from
  different shards arrive in no particular order relative to each other.
- **A shard that cannot be polled fails the poll** rather than being skipped,
  because a skipped shard looks exactly like an empty one.

It needs brokers that answer a group request for a shard they do not lead with
a redirect, which is any broker from this release on.

## Redirects

A subscribe or a consumer-group request sent to a broker that does not own the
shard is answered with a redirect naming the one that does. `ClusterClient::subscribe`
and the single-shard group calls (`ClusterClient::group_poll`, `group_poll_wait`,
`group_ack`, `group_nack`, `group_dead_letters`, `group_discard`,
`group_redrive`) follow it — up to three hops, never revisiting a broker within
one attempt, because a cluster mid-rebalance can otherwise bounce a client
between two brokers that disagree. The group calls remember which broker served
each shard and go straight there next time, until a call against it fails. This
is what keeps a consumer working after its shard moves: once the move cuts
over, the old leader answers group requests with a redirect to the new one.

If you use `Client` directly, the redirect surfaces as a typed
`NotLeaderError` carrying the owner's node id and address. It is an
instruction, not a failure. `Client` is one broker's connections and does not
follow it; connecting to the owner is `ClusterClient`'s job.

A **publish** to the wrong broker is *forwarded* rather than redirected, so it
needs nothing from you.

## Retries

Bounded by an attempt count and a jittered exponential backoff, and optionally
by a deadline.

- **The backoff is full jitter** — a delay uniform in `[0, ceiling]`, with the
  ceiling doubling to `max_backoff`. Every client of a cluster notices a
  failover at the same instant, and an unjittered backoff would send all of them
  at the freshly promoted broker together.
- **`deadline` is `None` by default, deliberately.** A deadline shorter than one
  attempt's own timeout prevents any retry at all: the first attempt spends the
  budget and the loop exits having tried once. Set it from your own latency
  budget, above the client's publish timeout, or leave it off.

What happens after a failure is decided by the retry class the broker sent with
it (see "Error codes" in `docs/protocol.md`):

| Class | Codes (usually) | `publish_at_least_once`, idempotent producer | `publish` |
| --- | --- | --- | --- |
| `fatal` | `unauthenticated`, `forbidden`, `invalid_request`, `limit_exceeded` | Returned at once, marked "not retried" | Returned |
| `retry`, `redirect` | `shard_unavailable`, `draining`, `not_leader`, `stale_claim` (group acks only; never reroutes) | From a cached owner or leader: forget it and go again at once through the entry broker. From the entry broker: back off and retry | From a cached owner: forget it and send once through the entry broker. Otherwise returned |
| `retry_after` | `overloaded`, `not_found` | Back off, at least `retry_after_ms` when the broker gave one. `not_found` only for 5 s from the first one | Returned |
| `outcome_unknown` | `quorum_timeout`, `leadership_lost`, `unacknowledged`, `internal`, `storage` | Sent again: that is what at-least-once means, and the idempotent producer's sequence makes it safe | **Returned, never sent again** |

- **A cached route is dropped as soon as it answers "nothing applied".** An
  owner that was fenced mid-move, or is draining, will not start serving the
  shard again, so backing off and asking it again only delays the publish. The
  entry broker routes by the current assignment.
- **`not_found` is retried, but only for 5 s.** A broker learns its streams from
  the control plane (every 2 s by default), so one promoted a moment ago
  reports the stream it is about to serve as missing. Past a couple of syncs the
  stream really is missing, and a large attempt budget should not hide that.
- **`publish` reconnects only when the broker is going away.** A coded answer
  means the broker is up; only `draining`, or no answer at all, replaces the
  client.
- A subscribe, cache watch or group request whose redirect target answers
  `shard_unavailable` or `draining` goes back to the entry broker once.

A broker that predates error codes sends prose, and the client falls back to
what it did before codes: only a credential failure (and a subscribe offset
retention has passed) is terminal, and everything else is retried, **including
"not found"** and errors it has never seen, because a wasted attempt is a
cheaper mistake than a lost operation. Such a peer never triggers the immediate
reroute.

One imprecision: a publish that the entry broker forwarded to an owner that was
fenced comes back as `shard_unavailable` with reason `not_ready`, not `fenced`,
because the broker-to-broker answer does not say which. The class, `retry`, is
right either way.

## Consistency, from the client's side

Set per stream on the control plane, not on the client.

- **`Leader`** — acknowledged once the owning broker has it durably. Fast, and
  loses whatever it had not yet replicated if that broker's storage is lost. The
  window is the broker's `felix_broker_replication_lag_records`.
- **`Quorum`** — acknowledged once a majority of the replica set holds it. A
  publish that cannot reach a majority comes back as `quorum_timeout`, retry
  class `outcome_unknown`. That is not the same as "it failed": the record may
  be on disk and may yet reach a majority. `publish` returns it; let
  `publish_at_least_once` resend if duplicates are acceptable, or use an
  idempotent producer, which resends without the duplicate.

**`DeliveryGuarantee` (`AtMostOnce` / `AtLeastOnce`) is declared on a stream and
not enforced by any broker.** Do not rely on it. What you get is what this page
and `semantics.md` describe.

## Errors you should expect

| What you see | What it means | What to do |
| --- | --- | --- |
| `BrokerError` with `forbidden` | The token lacks the permission | Fix the credential. Not retried |
| `BrokerError` with `not_found` | This broker does not know it *yet*, or at all | Retried for you for 5 s |
| `BrokerError` with `quorum_timeout` (`outcome_unknown`) | `Quorum` could not reach a majority in time | Unknown, not failed. Resend only if duplicates are acceptable, or use an idempotent producer |
| `BrokerError` with `shard_unavailable` | The shard is between owners, or its owner is not ready | Retried for you; a cached owner is dropped at once |
| `no eligible leader` / owner unavailable | No replica holds the log, so the shard is unavailable rather than served empty | Wait for the broker to return; only an operator can abandon the log |
| `shard is owned by …` (`NotLeaderError`) | You used `Client`, not `ClusterClient` | Follow it, or use `ClusterClient::subscribe` |
| `gave up after … attempts` | Every endpoint failed | The cluster is unreachable, not merely rebalancing |

## Trying it on three brokers

The cluster harness runs a real three-broker cluster with a control plane:

```bash
task cluster:failover
```

It starts three brokers, configures a `Quorum` stream, connects a client with
**one** address, shows it discovering the other two, kills the broker that
acknowledged the writes, and reads every record back from the broker that took
over. `crates/testing/felix-cluster/tests/` uses the same harness, and those tests are
the worked examples for everything on this page.
