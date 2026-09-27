---
title: "Control-plane high availability"
description: "Run several identical control-plane instances over one HA Postgres — what Felix handles, what the database platform must provide, and what a failover looks like."
---

The Felix control plane is deliberately stateless: every piece of durable
metadata — tenants, streams, membership, shard ownership, auth configuration —
lives in one Postgres database, and any number of identical instances serve
it. High availability is therefore two separate obligations:

- **The instances** — run two or more, behind anything that routes on the
  readiness probe. This half is Felix's, and it is proven by test: a rolling
  restart of every instance, with broker heartbeats and shard-assignment
  watches flowing throughout, serves every call.
- **Placement runs on one instance at a time**, the holder of a lease kept
  in the database. It is renewed on every pass, handed over when the holder
  shuts down, and taken over by another instance three reconcile intervals
  (15 s by default) after a holder dies; until then moves already started
  carry on but nothing new is placed on the timer. Every placement write is
  also fenced by a token in the same row, so the move limits hold across
  instances even while two of them think they are placing
  ([control-plane.md](https://github.com/gabloe/felix/blob/main/docs/control-plane.md)).
  `felix_placement_lease_held` summed across instances is 1.
- **The database** — Postgres availability is an operational input Felix
  consumes, **not something Felix implements**. If Postgres is down, every
  instance fails readiness and metadata is unavailable, however many are
  running.

## What Felix asks of the database

Each of these is load-bearing for a specific Felix behaviour, not a tuning
suggestion.

- **One writable primary behind one stable endpoint.** Every instance sends
  all reads and writes to its single `FELIX_CONTROLPLANE_POSTGRES_URL`. There
  is no read-replica routing and no multi-master tolerance: change feeds and
  shard-assignment `generation` counters assume a single writer.
- **Failover must not lose acknowledged commits.** A shard-assignment
  generation that rolls back can be reused for a *different* owner, and
  brokers de-duplicate ownership changes by generation — a reused one looks
  like a duplicate and is dropped. Run synchronous replication
  (`synchronous_commit = on` to a standby, or a quorum equivalent). At
  control-plane write rates the synchronous cost is negligible, so async
  replication trades away correctness for nothing.
- **The endpoint follows the primary.** After a failover the same URL must
  reach the new primary — a VIP, a proxy, or a Kubernetes service maintained
  by the operator. Felix instances just reconnect to the URL they were given.
- **Back up the whole database, restore it as a whole.** Metadata tables
  reference each other, and the migration ledger records which schema the data
  is in; a partial restore leaves instances refusing readiness or misreading
  rows. Point-in-time recovery is fine — any consistent point is a state
  Felix has actually been in.

## Supported topologies

- **Managed Postgres** (RDS/Aurora, Cloud SQL, Azure Database, …) with
  multi-AZ/HA enabled — verify the failover mode is synchronous ("zero data
  loss"), not async replica promotion.
- **Operator-managed Postgres on Kubernetes** — CloudNativePG or
  Patroni-based operators, with at least one synchronous standby, pointing
  Felix at the operator's *read-write* service (e.g. CloudNativePG's
  `<cluster>-rw`).

A single Postgres with no standby is fine for development; in production it
is a conscious decision that control-plane instances add nothing when the one
database is gone.

## What a failover looks like from Felix

No Felix-side action is required at any point — that is the design.

1. The primary fails; each instance's pooled connections start erroring.
2. Within a readiness window (answers cached `FELIX_READINESS_CACHE_TTL_MS`,
   default 1s; checks bounded by `FELIX_READINESS_TIMEOUT_MS`, default 2s)
   every instance fails `/v1/system/ready` and load balancers stop routing to
   all of them. **Liveness keeps passing** — an external outage must not
   trigger restarts that cannot fix it.
3. The Postgres platform promotes a standby and moves the endpoint.
4. Instances reconnect through the same URL; the first readiness check that
   succeeds puts each back in rotation.
5. Brokers retry heartbeats and keep their last-known catalog. They keep
   serving the shards they lead only while their lease holds (0.75 ×
   `FELIX_NODE_EXPIRY_TIMEOUT_MS` from the last accepted heartbeat), so with
   the defaults they stop serving those shards between about 5s and 11s into
   the outage.
6. Once the database is back, each broker's next heartbeat lands within about
   3s and renews its lease. The expiry sweep waits one full expiry window
   after it can read the store again, so the outage itself does not mark
   brokers down.

A promotion shorter than about 5s is usually absorbed by the lease; a longer
one makes shards unavailable for the rest of the outage but does not take
brokers out of the cluster. A broker that is marked down for any other reason
registers again on its next heartbeat.

## Probes

| Path | Question | Wire it to |
| --- | --- | --- |
| `/v1/system/live` | Should this process be restarted? Touches nothing outside the process. | liveness probe |
| `/v1/system/ready` | Should this instance get traffic? Fails on a store that does not answer, answers an error, or is on an older schema than the build expects. | readiness probe |

Recommended settings: readiness `periodSeconds: 2–5` (answers are cached 1s,
so polling faster costs nothing and gains nothing), `timeoutSeconds: 3`
(above the 2s internal bound, so the reason is reported rather than lost),
`failureThreshold: 2–3`; liveness `periodSeconds: 10`, `failureThreshold: 3`.
**Liveness must never check the database** — a restart is the one response an
external outage does not deserve, and Felix's `/v1/system/live` deliberately
answers from memory.

Both halves are proven against a real database, not asserted: a test cuts
connectivity beneath the connection pool and checks that readiness turns `503`
while liveness stays `200`, that readiness returns by itself when connectivity
does, that an instance running ahead of its migrations stays out of rotation,
and that a probe meeting a database which accepts connections but never answers
still comes back inside its own bound instead of hanging.

On SIGTERM an instance fails readiness first and keeps serving for
`FELIX_SHUTDOWN_PREDRAIN_MS` so load balancers can act on it, then drains
against `FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS` — the whole sequence is on
[Graceful Shutdown](/felix/deployment/graceful-shutdown/).

## Sizing and connections

Each instance opens one pool of
`FELIX_CONTROLPLANE_POSTGRES_MAX_CONNECTIONS` (default 10), shared by request
handlers, the readiness probe (at most one query per second, thanks to the
cache), the node-expiry sweep, and the shard reconciler. For N instances,
provision Postgres `max_connections` as:

```
N × pool size + platform overhead (replication, backups, admin) + ~20%
```

Adding instances scales *availability*, not database throughput — the
database stays the shared bottleneck, which at control-plane rates it is
nowhere near.

## Migrations and rolling deploys

Every instance runs the embedded migrations at startup under an advisory
lock, so concurrent starts are safe and there is no migration job to
orchestrate. Migrations are additive; readiness fails only when the database
is *behind* the running build, never when it is ahead — during a rolling
deploy the first new instance migrates the schema forward and old instances
keep serving. The operator rule that falls out: **roll forward, not back**. A
build older than the schema keeps working; a pre-migration backup restored
under a newer build fails readiness until the migrations rerun on the next
start.

## Whose failure is whose

| Failure | Handled by |
| --- | --- |
| An instance dies or is deployed | Felix — survivors serve; drain and readiness make it invisible |
| Transient connection loss, pool exhaustion | Felix — readiness fails and recovers on its own; brokers retry |
| Primary failover, replication, endpoint movement | The Postgres platform |
| Durability of committed metadata | The Postgres platform (synchronous replication) |
| Backups and point-in-time recovery | The Postgres platform |
| Schema migrations and cross-version readiness | Felix |

The full contract is in
[`docs/ha-postgres.md`](https://github.com/gabloe/felix/blob/main/docs/ha-postgres.md),
including why the Felix-owned alternative was deferred and what changed. That
alternative has since shipped: if operating a Postgres is the part you would
rather not, [Metadata Raft](/felix/architecture/metadata-raft/) holds the same
metadata in the control-plane instances themselves.
