---
title: "Use Cases"
description: "The ways Felix is used, what each one is built from, and how it compares with the systems usually used for it."
---

Most of the scenarios below usually take two or three systems: a database or
coordination store for the current value, a broker for the changes, and a
fanout tier to reach the consumers. Each one has its own failure modes, and the
gaps between them (the update published before the write committed, the change
that landed between a read and a subscribe) are where bugs live.

Felix serves streams, caches, queues and counters from one replicated log, so
the current value and the changes to it share one order and one set of
offsets. A consumer reads the state and follows every change after it without
a gap. A slow consumer never holds up a publisher or anyone else. Every
operation is scoped to a tenant, and every connection is QUIC with TLS 1.3.

Each page shows how the scenario is usually built, why Felix fits it, working
code, a few commands to try on your machine, a comparison with the
systems people reach for first, and where Felix stops. Where a scenario depends
on a capability that is still partial, the page says what is missing, using the
status markers from [What Felix Is For](/getting-started/what-felix-is-for/).

## Keeping many consumers in sync

These are the scenarios Felix is designed around: a large number of processes
that each need an accurate local copy of state that keeps changing.

| Scenario | What you get | Usually built with |
|---|---|---|
| [Config and feature flags](/use-cases/config-and-feature-flags/) | Every consumer reads the current value, then receives every change after it with no gap, resumable by offset | etcd or Consul watches, a watch cache in front, polling |
| [Live updates at high fanout](/use-cases/live-fanout/) | One publish encoded once for every subscriber, and a slow subscriber never slows the publisher or anyone else | NATS core, Redis pub/sub, Kafka with one group per consumer |
| [Browsers and mobile clients](/use-cases/browser-realtime/) | Browsers reach Felix through a WebSocket gateway, with each connection's token narrowed to the keys it may see | A hand-built WebSocket tier in front of a broker |

## Coordinating work

| Scenario | What you get | Usually built with |
|---|---|---|
| [Leases, locks and idempotency](/use-cases/leases-and-locks/) | Compare-and-set writes with TTLs that survive a restart, and watches that see a lease lapse | Redis `SET NX PX`, etcd leases, ZooKeeper |
| [Rate limits and counters](/use-cases/rate-limits-and-counters/) | A durable increment that returns the new total in one round trip | Redis `INCR`, a database row |
| [Work queues](/use-cases/work-queues/) | Consumer groups with visibility timeouts, redelivery and dead letters on the same log as your streams | SQS, RabbitMQ, Kafka consumer groups |

## Recording what happened

| Scenario | What you get | Usually built with |
|---|---|---|
| [Events and state together](/use-cases/event-and-state/) | An event and the state it changes written as one record, with no outbox | Postgres plus an outbox table plus Kafka |
| [Audit logs and replay](/use-cases/event-log-and-replay/) | A crash-safe log with offsets, append times, and the identity of whoever wrote each record | Kafka, JetStream, a database audit table |
| [Coming from Kafka](/use-cases/kafka-migration/) | Kafka producers, and consumers that assign their own partitions, read and write durable streams; no consumer groups or transactions | Kafka |

## Running it for others

| Scenario | What you get | Usually built with |
|---|---|---|
| [Multi-tenant SaaS](/use-cases/multi-tenant-saas/) | Tenant scoping on every operation, tenant-scoped tokens, per-tenant publish limits | Kafka ACLs and topic prefixes, NATS accounts |
| [Data residency](/use-cases/data-residency/) | Streams whose leader and replicas stay in one region | Separate clusters per region |

## Try it

The [Quickstart](/getting-started/quickstart/) runs a broker, a control plane
and `felixctl` with one `docker compose up`. Every page below has a short "Try
it" section that runs against that stack.

## When to use something else

Felix is pre-1.0. If you need a mature connector ecosystem, stream processing
or exactly-once pipelines across topics, Kafka is the better choice today. If
you need a small, strongly consistent store for cluster coordination and do not
need fanout, etcd is simpler to run. [Why Felix?](/getting-started/why-felix/)
covers this in more detail.
