---
title: "Data Residency"
description: "Pin a stream's data to a region with placement the control plane enforces, spread copies across failure zones, and see exactly which residency guarantees Felix does not yet make."
---

A European customer's order events must stay on brokers in the EU. The same
company runs a US cluster for US customers and wants one control plane, one set
of clients and one operations team for both. Inside each region, the three
copies of a shard should not share a rack or an availability zone. A regulator
asks where the data is, and the answer has to be something the system enforces.

This page covers a capability that is 🚧 Partial in
[What Felix Is For](/getting-started/what-felix-is-for/). Zone-aware placement is
✅ Today. Region pinning works for streams and has gaps, listed under
[Where it stops](#where-it-stops).

## How this is usually built

The common answer is one cluster per jurisdiction. Each region runs its own
Kafka or NATS cluster, and an application routes each tenant's traffic to the
right one. It is easy to audit, because the boundary is a network boundary, and
it costs you N sets of operations, N sets of credentials and application code
that knows the map. Kafka can place replicas by rack with `broker.rack`, and
cross-region copying is a separate tool (MirrorMaker or a replicator) that you
configure to move only what you intend to. NATS has superclusters and leaf nodes,
and JetStream streams can be placed on a named cluster or tagged servers. These
are real and mature. What they ask is that the residency boundary lives in how you
wired the clusters, not in a per-stream property.

## Why Felix fits

Region is a property of the stream, not of the wiring. The control plane applies it at every point where it chooses where a shard lives: first placement, rebalancing, drains, failover promotion and operator moves. An operator move outside the allowed regions is refused as `region_not_allowed`, so a routine drain cannot quietly carry data across a border.

A copy found outside its allowed regions, because a broker's region changed or a bridge was removed, is moved back the way a draining broker's copies are.

Brokers also refuse to forward to a leader outside their region and its bridges, with `shard_unavailable` and reason `region_not_routable`. The `a_stream_homed_in_one_region_is_refused_by_another` test checks this with two regions and no bridge.

One control plane, one set of clients and one operations team cover every region, and tenants stay separately addressed and authorized on shared brokers. Cross-region replication happens only where an operator wrote a directional bridge such as `eu-west-1>us-east-1`.

Inside a region, `FELIX_NODE_ZONE` spreads each shard's followers across failure zones and keeps the spread through drains and rebalances. A shard that cannot be spread is placed anyway and raises `felix_shards_zone_unspread`, and a dry-run move reports `zones_before` and `zones_after`.

## How it works on Felix

A stream created with a `region` has its leader and every replica only on brokers
that report that region (`FELIX_REGION_ID` on the broker), or in a region the
directional allowlist `FELIX_REGION_BRIDGES` bridges it to. Bridges are
`source>dest` pairs such as `eu-west-1>us-east-1`; a two-way bridge is two
pairs. With no bridge, a stream stays in its own region.

The control plane enforces this wherever it chooses where a shard lives: first
placement, rebalancing, drains, failover promotion and operator moves. An
operator move to a broker outside the allowed regions is refused as
`region_not_allowed`. A copy found outside the allowed regions, because a
broker's region changed or a bridge was removed, is moved back the way a draining
broker's copies are. A broker forwards a request only to a leader in its own
region or one its bridges reach, and otherwise refuses it as
`shard_unavailable` with reason `region_not_routable`. The region is fixed at
creation.

Within a region, a broker registers its failure domain with `FELIX_NODE_ZONE`, and
placement puts each shard's followers in zones the shard has no copy in yet.
Drains and rebalances keep the spread, and a follower that shares a zone with
another copy is replaced once a broker in a missing zone has room. If a shard
cannot be spread, it is placed anyway with a warning and the metric
`felix_shards_zone_unspread`. An operator's move is never refused on zone grounds;
a dry run reports `zones_before` and `zones_after`.

Everything is also scoped by tenant. A token for one tenant is useless against
another, and the broker rejects unknown tenants, so a tenant's data is addressed
and authorized separately from every other tenant's even on shared brokers.

## Example

Brokers report their region, and the control plane and every broker share the
same bridge list:

```bash
FELIX_REGION_ID=eu-west-1 FELIX_NODE_ZONE=eu-west-1a felix-broker
FELIX_REGION_ID=us-east-1 FELIX_NODE_ZONE=us-east-1b felix-broker
FELIX_REGION_BRIDGES=eu-west-1>us-east-1   # unset to keep each stream in its own region
```

Create a stream pinned to the EU. The region is fixed at creation:

```bash
felixctl stream create orders-eu --shards 4 --replication 3 --consistency quorum \
  --region eu-west-1
felixctl stream info orders-eu
```

Over the REST API it is a field of the create call:

```http
POST /v1/tenants/t1/namespaces/payments/streams

{ "stream": "orders-eu", "kind": "Stream", "shards": 4, "replication_factor": 3,
  "retention": { "max_age_seconds": null, "max_size_bytes": null },
  "consistency": "Quorum", "delivery": "AtLeastOnce", "durable": true,
  "region": "eu-west-1" }
```

Preview an operator move before you make it. A move outside the allowed regions
is refused:

```bash
felix-controlplane admin move t1/payments/orders-eu/0 broker-us-1 --dry-run
```

See [Moving shards](/deployment/moving-shards/) for the arguments. The
`a_stream_homed_in_one_region_is_refused_by_another` test runs two regions with no
bridge and checks that each refuses the other's stream.

## Compared with a cluster per region, Kafka and NATS

A separate cluster per jurisdiction is the strongest boundary available, because it is a network boundary, and it covers every kind of data including caches and counters, which Felix cannot pin today. It costs N sets of operations and credentials, and application code that knows which cluster holds which tenant. Felix puts the region on the stream and has the control plane enforce it across placement, drains and failover, so the clients and operations stay single. Pick separate clusters when the requirement is a hard network boundary or covers caches.

Kafka places replicas by rack with `broker.rack`, and moving data between regions is a separate tool such as MirrorMaker that you configure to move only what you intend. Felix does the equivalent of rack awareness with `FELIX_NODE_ZONE`, and the region limit is part of the stream's definition, not a replicator's topic list.

NATS has superclusters and leaf nodes, and JetStream streams can be placed on a named cluster or on servers matching tags. Those are mature. Felix's difference is that the region is checked by the control plane on every later placement decision (failover, rebalancing, drains) and a stray copy is moved back. Felix lacks a bridge agent, per-region encryption and a cross-region audit log, as listed below.

## Try it

Placement by region needs brokers that report one, and the one-command stack runs
a single broker with none. The example above starts brokers in two regions and
creates the pinned stream; `felixctl stream info orders-eu` then shows where its
shards landed.

## Where it stops

This is the list the status table gives, and each item is a way a residency claim
can fail today.

A stream created without a region is placed anywhere. Every cache is placed
anywhere, because caches take no region. Config or session data that must stay
in a jurisdiction cannot be pinned with this feature, and neither can counters.

The allowlist is per process. The control plane and each broker have to be given
the same `FELIX_REGION_BRIDGES`, and nothing checks that they agree. Brokers ship
to the replica set the control plane assigned without checking its regions
themselves, so the control plane's placement is the only guard on replication.

A follower already outside the region stays there until a broker inside the
region can replace it. A client that connects straight to a leader is not asked
where it is, so where a client connects is not checked against the stream's region.

Felix has no bridge agent, so permitted cross-region replication is whatever the
replica set does, with no separate stage that filters or transforms what leaves a
region. There is no per-region encryption and no audit log of cross-region
movement. Without those you can show where the control plane placed the data,
and you cannot produce a log of every time it crossed a border.

Zone spread inside a region works as described, with one limit: a broker's zone is
read when it registers, so changing it takes a restart.

If you need a residency boundary that is a hard network boundary today, or a
cache pinned to a region, run a separate Felix cluster per jurisdiction.

## Related

- [What Felix Is For](/getting-started/what-felix-is-for/) for the status rows
- [Environment variables](/reference/environment-variables/) for `FELIX_REGION_ID`, `FELIX_REGION_BRIDGES` and `FELIX_NODE_ZONE`
- [Control plane API](/api/control-plane-api/) and [Moving shards](/deployment/moving-shards/)
- [Security](/features/security/#multi-tenancy-and-isolation) for tenant scoping
