---
title: "Demo: Multi-tenant Real-time Notifications"
---

## What this shows

- Tenant isolation (t1 vs t2) enforced by broker auth
- Fanout to multiple subscribers per tenant
- Cache snapshots of the last N alerts (`last_alerts`)
- Failure injection: drop and restart a subscriber

## Notes

- This demo starts an in-process broker and QUIC server on a random local port.
- You do not need to run the broker separately.

## Architecture

```mermaid
flowchart TD
    CP["Control Plane<br/>(demo auth)"]
    C["Client<br/>tenants t1 / t2"]
    B["Broker<br/>QUIC"]
    S["Subscribers<br/>2 per tenant"]
    K["Cache (KV)<br/>last_alerts"]

    CP -->|"Felix token"| C
    C -->|publish| B
    B -->|events| S
    B --> K
```

## Run

```bash
task demo:notifications
# or
cargo run --release -p felix-broker-service --bin pubsub-demo-notifications
```

Optional failure injection:

```bash
cargo run --release -p felix-broker-service --bin pubsub-demo-notifications -- --drop-subscriber
```

## Configuration flags

- `--alerts=10`: total alerts per tenant (default: 10)
- `--last-n=5`: cache window size per tenant (default: 5)
- `--drop-subscriber`: drop and restart one subscriber mid-run

## Expected output (sample)

```
== Felix Demo: Multi-tenant Real-time Notifications ==
Step 3/8: opening fanout subscriptions.
Cross-tenant subscribe blocked as expected.
Step 5/8: publishing alerts + updating cache (last N=5).
[t1/sub-a] event on alerts: t1 alert #1
[t1/sub-b] event on alerts: t1 alert #1
[t2/sub-a] event on alerts: t2 alert #1
[t2/sub-b] event on alerts: t2 alert #1
Failure injection: dropping t1/sub-b mid-run.
Restarting t1/sub-b to show resume behavior.
Step 7/8: reading cache snapshots.
[t1] last alerts: ["t1 alert #6", "t1 alert #7", "t1 alert #8", "t1 alert #9", "t1 alert #10"]
[t2] last alerts: ["t2 alert #6", "t2 alert #7", "t2 alert #8", "t2 alert #9", "t2 alert #10"]
Demo complete.
```

## Failure injection

- `--drop-subscriber`: drops one subscriber mid-run and restarts it.
- This demonstrates that other subscribers continue receiving events and that
  the restarted subscriber resumes from new events only. That is a property of
  *this* demo's streams, which are not durable. A durable stream can be resumed
  from a checkpointed offset instead. See
  [durable storage](/architecture/durable-storage/).

## How to extend

- Add a third tenant with custom namespaces and different RBAC policies.
- Emit alert categories and filter on the client side.
- Persist last N alerts in a real cache backend and compare behavior.
