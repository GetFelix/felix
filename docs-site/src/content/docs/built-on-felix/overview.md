---
title: "Built on Felix"
description: "Applications that use Felix as their whole backend, and which Felix features each one depends on."
---

These are separate projects in the [GetFelix](https://github.com/GetFelix)
organization. Each uses Felix as its only backend, with no database next to
it, and each was written partly to find out where Felix falls short for a real
application. The pages here say which Felix features each one uses and what it
relies on Felix to guarantee. Setup and development detail lives in each
repository.

| Project | What it is | Felix features it leans on |
|---|---|---|
| [felix-canvas](/built-on-felix/canvas/) | A multiplayer whiteboard | Durable and in-memory streams, offsets to detect drops, a snapshot cache, a TTL presence cache, a consumer group, felix-gateway |
| [felix-webhook-relay](/built-on-felix/webhook-relay/) | A webhook relay with retries, dead letters and replay | Durable streams, the idempotent producer, one consumer group per endpoint, watched config caches, counters, replay by offset |
| [felix-arena](/built-on-felix/arena/) | A browser team deathmatch, played through Felix, with a kill cam replayed from the log | Atomic commits, replay from an offset for a kill cam, quorum replication, per-arena token narrowing |
| [felix-metro](/built-on-felix/metro/) | A 3D city simulator, in design, whose timeline goes back to any moment | A shard per district, atomic commits with keyframe state, range reads to seek, subscribe from an offset into live, expected-offset fencing |

The browser apps reach Felix through
[felix-gateway](/clients/browsers/), which is documented with the clients.
