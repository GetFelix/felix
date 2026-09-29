# Subscribe routing: redirect, not proxy

**Decision: a subscription for a shard this broker does not own is answered with
a redirect to the owner. Proxying is rejected as the default.**

Recorded for M4.4 (#107). The rejected alternative and the measurements that
informed the choice are both below, because the reasoning matters more than the
verdict if this is ever revisited.

## Status

Implemented (#118). A broker answers `NotLeader` for a shard it does not own,
carrying the owner's node id, its client-facing address, and the generation;
`ClusterClient::subscribe` follows it, capped at three hops and refusing to
revisit a broker within one attempt. A client that did not offer
`FEATURE_REDIRECT` gets an ordinary error instead, because a message it cannot
decode would cost it the connection.

## The two designs

**Redirect.** A broker that does not own the shard answers `NotLeader`, carrying
the owner's node id, advertised address, and generation. The client reconnects
to the owner. The data path is owner → client, exactly as it is today.

**Proxy.** The receiving broker subscribes upstream to the owner and relays
events to its local subscribers. The client never learns the topology. The data
path is owner → proxy → client.

## What the measurement says

One store-and-forward hop, in process, 20k events per run, one publisher, a
single consumer timed against the publish that produced its event.

Paced at one publish per 50µs, so neither path is queue-bound:

| fanout | path | p50 | p99 | p999 | delivered |
| --- | --- | --- | --- | --- | --- |
| 1 | direct | 20µs | 80µs | 327µs | 20000/20000 |
| 1 | proxied | 20µs | 93µs | 394µs | 20000/20000 |
| 10 | direct | 18µs | 93µs | 568µs | 20000/20000 |
| 10 | proxied | 21µs | 84µs | 528µs | 20000/20000 |
| 100 | direct | 19µs | 72µs | 381µs | 20000/20000 |
| 100 | proxied | 23µs | 70µs | 276µs | 20000/20000 |

Unpaced, publisher running flat out:

| fanout | path | p50 | p999 | delivered |
| --- | --- | --- | --- | --- |
| 1 | direct | 4µs | 152µs | 20000/20000 |
| 1 | proxied | 886µs | 1.108ms | 15355/20000 |
| 10 | direct | 4µs | 125µs | 20000/20000 |
| 10 | proxied | 937µs | 1.105ms | 15078/20000 |

Three repeats agreed on the paced figures. Two findings, and the first
corrected an assumption that had been driving the argument:

**The hop is nearly free on latency.** With queues shallow it costs 1–4µs at p50
and is indistinguishable at p99 and p999. At fanout 10 and 100 the proxied tail
came in *lower* than direct, which is noise, not an effect. The intuition that an
extra queue must show up in p999 was wrong, and the numbers say so plainly.

**The cost is capacity, and it is paid in loss.** At full publisher rate the
direct path delivered everything and the proxied path dropped 23%, at a p50 two
hundred times worse. That is the second bounded queue reaching its limit and
`DropNew` doing what it is configured to do.

Two caveats on that number. The relay is unbatched, so a production proxy would
have a higher ceiling; the 23% is not a fixed property of proxying. And the
measurement is in process, so it excludes the second QUIC hop entirely, so the
latency column is a lower bound.

What survives both caveats is structural: proxying adds a second bounded queue
to the delivery path, so *some* publish rate exists above which it drops while
the direct path does not. Batching moves that rate; it does not remove it.

## Why proxying is rejected

The measurement is corroborating. The decisive argument is that proxying forces
a choice between two designs, and each contradicts something Felix claims:

**One upstream subscription per (proxy, shard).** Cross-broker traffic stays at
one stream, which is the only affordable option at high fanout. But that
subscription is *shared*: every local subscriber for that shard is behind one
queue. A slow consumer either gets dropped locally, or backpressures the shared
stream and degrades everyone else on that proxy. Felix's one-line description
promises strict slow-consumer isolation; this makes it "isolated within a
broker, shared fate across the hop".

**One upstream subscription per local subscriber.** Isolation is preserved and
fanout amortisation is destroyed. `DeliveryEnvelope` caches its encoded frame so
a publish is encoded once regardless of fanout; this design pays cross-broker
bandwidth per subscriber instead. At fanout in the thousands that is not a
tuning problem, it is a different product.

There is no third arrangement. The first is the only viable one, and it trades
away the property the project leads with.

## What redirect costs

Not free, and these are the reasons to revisit it:

- **Rebalance churn.** Every ownership change disconnects and reconnects the
  affected subscribers, at exactly the moment the cluster is already under
  stress. A proxy absorbs the move invisibly. This is the strongest argument for
  proxying and it is unmeasured; M9 should quantify a mass reconnect at fanout
  before rebalancing ships.
- **Client complexity.** The client must discover topology, follow redirects, and
  reconnect across failover (M6: #117, #118, #119). Every future non-Rust SDK
  inherits that.
- **Deployment reach.** A client behind a load balancer or NAT that can reach
  only one endpoint cannot follow a redirect at all.
- **Connection count.** A client subscribing across B brokers holds up to B
  connections rather than one.

## Behaviour this commits to

- **A subscribe for an unowned shard is answered `NotLeader`**, never served
  locally and never silently accepted.
- **Ownership changing under a live subscription** ends that subscription with
  `NotLeader` naming the new owner. The client reconnects there and resumes from
  its last offset, which durable streams already support; what it must not do is
  keep receiving from a broker that no longer owns the shard.
- **A redirect names a generation.** A client that is handed an older generation
  than it already knows treats it as stale and retries rather than following it
  backwards.
- **Nothing falls back to proxying.** An unroutable shard is a typed error, and
  the absence of a silent fallback is the point: a proxy that appeared under load
  would move the failure from visible to invisible.
- **Cache watches follow the same decision.** A `cache_watch` for a shard this
  broker does not own is redirected, not proxied, through the same dispatch the
  subscribe path uses. A watch served off the owner would go quiet on writes it
  cannot see, which is the served-locally failure in a different costume.

## What this requires

**Wire.** The client protocol has no way to say "not here, go there". Adding one:

- A `NotLeader` variant on `Message`, carrying node id, advertised address, and
  generation, mirroring the internal kind of the same name.
- A negotiated bit for it. `Message` is `#[serde(tag = "type")]`, so an unknown
  variant fails to decode rather than being ignored: a client that predates this
  must be answered with the existing `Error` instead. The bit is offered in
  `Auth.client_flags` and confirmed in `AuthOk.server_flags`, and it is a new bit
  (`ORIGINAL_V1_FLAGS` stays frozen).
- Nothing else. No frame-layout flag, because the payload of an existing frame
  does not change; no version bump, because negotiation is the mechanism.

**Client.** All of it in M6 (#117, #118, #119):

- Follow a redirect: connect to the named address, re-authenticate, re-subscribe.
- Resume at the last offset it saw, so a redirect mid-stream is not a gap. This
  needs `StartPosition`, which already exists.
- Bound the following. A redirect chain must terminate, and a redirect back to a
  generation the client has already left is refused rather than followed.
- Hold a connection per owner it is subscribed to, and reuse it across
  subscriptions to the same broker rather than opening one per stream.

**Broker.** The subscribe handler consults the same `IngressRouter::dispatch`
the publish path uses, so both agree on ownership by construction, and answers
`NotLeader` on anything but `Dispatch::Local`.

## If this is revisited

Proxying is additive. `NotLeader` is already the redirect primitive, and relay
kinds slot into the internal protocol without a version bump, since an unknown kind
is already a typed error rather than a misparse. The likely trigger is
deployment reach rather than performance: a single-endpoint environment where
clients cannot reach brokers directly. In that case the honest shape is an
explicit opt-in mode with the shared-fate cost documented, not a default.
