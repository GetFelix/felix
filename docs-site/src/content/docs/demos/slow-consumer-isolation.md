---
title: "Demo: Slow-consumer Isolation"
---

```mermaid
flowchart LR
    P["Publisher"] e0@--> B["Broker<br/><small>one shared encoded batch</small>"]
    B e1@--> Q1["queue"] e2@--> S1(["dashboard A<br/><small>draining</small>"])
    B e3@--> Q2["queue"] e4@--> S2(["dashboard B<br/><small>draining</small>"])
    B e5@--> Q3["queue<br/><b>full</b>"] -.->|"dropped"| S3(["dashboard C<br/><small>stalled</small>"])

    e0@{ animate: true }
    e1@{ animate: true }
    e2@{ animate: true }
    e3@{ animate: true }
    e4@{ animate: true }
    e5@{ animate: true }

    classDef ok fill:#e9f5ec,stroke:#4a8a5e,color:#16301f
    classDef bad fill:#fdeaea,stroke:#b04a4a,color:#3d1414
    classDef step fill:#e8f0fe,stroke:#4a6fa5,color:#1a2b40
    class P,B step
    class Q1,Q2,S1,S2 ok
    class Q3,S3 bad
```

The animated edges are the ones still moving. Traffic keeps flowing to A and B
at full rate while C's bounded queue overflows and its events are dropped. The
publisher is never slowed by the slowest consumer. The broker counts the drops
(`felix_subscribe_dropped_total`), but on this ephemeral stream C itself cannot
tell it missed anything.

## What this shows

- One slow consumer does not degrade the healthy ones, the property in Felix's
  one-line description of itself
- The same workload under both subscriber queue policies, side by side
- What each policy actually costs, measured rather than asserted
- Sequence-gap counting, so "lost events" is a number rather than a claim

This is the demo to run first if you want to know why Felix exists rather than
what it can do. Then run its counterpart,
[Local State Divergence](/demos/state-divergence/), which shows what this trade-off
costs a consumer that holds a local copy of state.

## The question it answers

Every pub/sub system has to decide what happens when one subscriber stops keeping
up. There are only two honest answers, and they are both bad in different ways:

- **Drop** for that subscriber. Everyone else is unaffected; the slow one loses data.
- **Block** until it catches up. Nobody loses data; everyone slows to its speed.

Most systems pick one and bury it in a config file. Felix's default is `drop_new`,
and this demo runs both so the trade-off is visible instead of theoretical.

## Notes

- Starts an in-process broker and QUIC server on a random local port. You do not
  need to run a broker separately.
- Every event carries its own sequence number and publish timestamp in its
  payload. Events on an ephemeral stream carry no offset, so this is what makes
  gaps observable. A consumer otherwise cannot tell it missed anything.
- Numbers are single-node over loopback at fanout 3. They say nothing about
  behaviour at thousands of subscribers or across a real network.
- **Lost events are gone in this configuration.** The demo publishes to an
  ephemeral stream and drops on overflow, which is at-most-once by choice: there
  is no offset to rewind to and nothing redelivers. That is the point, because
  it is what makes the drops observable. Felix offers stronger options: a
  durable stream replays by offset, and a queue redelivers on visibility
  timeout with bounded attempts and a dead-letter destination. Picking one of
  those is what the [state divergence](/demos/state-divergence/) demo
  measures the cost of.

## Architecture

```mermaid
flowchart TD
    P["Publisher<br/>paced at --rate"]
    B["Broker<br/>fanout to all subscribers"]
    D1["dash-1<br/>healthy"]
    D2["dash-2<br/>healthy"]
    D3["dash-3<br/>stalls mid-run,<br/>stops draining"]

    P -->|"QUIC (TLS 1.3)"| B
    B --> D1
    B --> D2
    B --> D3

    classDef healthy fill:#0f766e,stroke:#2dd4bf,color:#ffffff
    classDef degraded fill:#b91c1c,stroke:#ef4444,color:#ffffff
    class D1,D2 healthy
    class D3 degraded
```

## Run

```bash
task demo:slow-consumer
# or
cargo run --release --manifest-path demos/slow-consumer/Cargo.toml
```

A terminal UI renders live. If stdout is not a terminal the demo falls back to
plain text automatically, so piping it or running it in CI works without flags.

## Configuration flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--rate N` | `20000` | Target publish rate, messages/sec |
| `--subscribers N` | `3` | Consumer count; must be at least 2 |
| `--payload N` | `256` | Payload bytes |
| `--queue-capacity N` | `512` | Subscriber queue depth (the broker default) |
| `--duration N` | `5` | Seconds per phase; a run is 4 phases |
| `--policy P` | `both` | `drop_new`, `block`, or `both` |
| `--no-tui` | off | Plain text instead of the terminal UI |

## Expected output (sample)

```
  policy = drop_new
    publisher           20000 msg/s achieved   (target 20000/s)
    dash-1             237701 received           0 lost
    dash-2             237701 received           0 lost
    dash-3             117462 received      120239 lost   <- stalled

  policy = block
    publisher           16419 msg/s achieved   (target 20000/s)
    dash-1             198703 received           0 lost
    dash-2             198703 received           0 lost
    dash-3             198703 received           0 lost   <- stalled
```

Read the two blocks together:

- Under `drop_new` the publisher hits its target and the healthy consumers lose
  nothing. The stalled consumer loses 120k events permanently.

  On a heavily loaded machine the healthy consumers may shed a handful of events
  themselves. Their own client-side queue is bounded too, and that has nothing to
  do with the stalled consumer. Isolation is a claim about orders of magnitude: in
  one CI run a healthy consumer shed 3 events out of 33,933 while the stalled one
  shed 20,108. The demo's test asserts that separation rather than perfection.
- Under `block` nothing is lost anywhere, and the publisher drops to 16.4k/s while
  the healthy consumers receive 198k instead of 237k. One sick consumer slowed
  everyone, and all three finish in lockstep.

Neither is the correct answer. Quoting
[how Felix works](/development/how-felix-works/): *"dropping isolates healthy
publishers and subscribers from a slow consumer; blocking preserves delivery but can
let one slow subscriber throttle every producer of that stream."*

## Configuring the trade-off yourself

The policy is not one switch. Backpressure has to propagate through the whole
chain, and each checkpoint has its own knob and its own default:

| Checkpoint | Setting | Default |
| --- | --- | --- |
| Broker ingress queue | `pub_ingress_wait` | `false` (sheds) |
| Broker subscriber queue | `subscriber_queue_policy` | `drop_new` |
| Broker writer lane | `subscriber_lane_queue_policy` | `drop_new` |
| Client subscription queue | `client_sub_queue_policy` | `drop_new` |

Leaving any of them on the shedding default means loss happens there first and the
ones downstream never matter. The lossless configuration requires all four. See
[backpressure internals](/development/internals-concurrency/) for the full set
of six checkpoints and why each exists.

## Failure injection

The stall is the injection: the consumer holds its subscription open and stops
calling `next_event`, which is what a blocked render loop, a paused container, or a
degraded link looks like from the broker's side. It recovers in the final phase so
you can see it resume and, under `drop_new`, see that the gap in what it received
is permanent.

## How to extend

- Raise `--subscribers` to see whether isolation holds as fanout grows.
- Lower `--queue-capacity` to make shedding start sooner.
- Stall more than one consumer by editing `run_once` in `src/scenario.rs`, which
  currently marks only the last subscriber as the victim.
- Set `--policy drop_new --rate` high enough to trigger ingress shedding, which
  shows up as a small loss shared by *all* consumers. That is a different
  checkpoint from the one this demo is about.
