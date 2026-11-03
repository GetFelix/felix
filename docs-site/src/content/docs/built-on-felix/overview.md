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

| Project | What it is | Leans on |
|---|---|---|
| [felix-canvas](/built-on-felix/canvas/) | A multiplayer whiteboard | Streams, a snapshot cache, a TTL presence cache |
| [felix-webhook-relay](/built-on-felix/webhook-relay/) | A webhook relay with retries, dead letters and replay | Durable streams, [consumer groups](/features/queues/), the idempotent producer |
| [felix-arena](/built-on-felix/arena/) | A browser team deathmatch with a kill cam replayed from the log | Atomic commits, replay from an offset |
| [felix-metro](/built-on-felix/metro/) | A playable 3D city simulator whose timeline goes back to any moment | A shard per district, range reads, subscribe from an offset |
| [felix-blocks](/built-on-felix/blocks/) | A voxel building game, early, that stores its world in Felix | A cache with one key per chunk, conditional writes |
| [felix-agents](/built-on-felix/agents/) | A swarm of AI workers, early, whose runs can be watched and replayed | A consumer group, conditional cache writes, RBAC |
| [felixd](/built-on-felix/felixd/) | A node agent that runs Wasm workloads and moves them when a node dies | Leases, `publish_if`, subscribe from an offset |

The browser apps reach Felix through
[felix-gateway](/clients/browsers/), which is documented with the clients.
