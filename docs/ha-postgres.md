# The Postgres the control plane requires

The Felix control plane is deliberately stateless: every piece of durable
metadata (tenants, streams, membership, shard ownership, auth configuration)
lives in one Postgres database, and any number of identical instances serve it.
That buys a simple availability story for the service itself (run two or more
instances; see [control-plane.md](control-plane.md)) at the price of an
explicit dependency: **Postgres availability is an operational input Felix
consumes, not something Felix implements.** If Postgres is down, every
instance fails readiness and metadata is unavailable, however many instances
are running. This document says what that dependency actually requires.

## What Felix asks of the database

These are contract items, not tuning suggestions. Each one is load-bearing for
a specific Felix behaviour.

- **One writable primary, reachable through one stable endpoint.** Every
  instance is configured with a single `FELIX_CONTROLPLANE_POSTGRES_URL` and
  sends all reads and writes there. There is no read-replica routing and no
  multi-master tolerance: the change feeds and shard-assignment `generation`
  counters are sequences that assume a single writer.
- **Failover must not lose acknowledged commits.** Felix's correctness leans on
  writes staying written: a shard-assignment generation that rolls back can
  re-issue an already-used generation for a *different* owner, and brokers
  de-duplicate ownership changes by generation, so a reused one is
  indistinguishable from a duplicate and is dropped. Run synchronous
  replication (`synchronous_commit = on` to a standby, or quorum-based
  equivalents) for the metadata database. Asynchronous replication trades this
  guarantee for latency; with the control plane's write rates (heartbeats and
  occasional CRUD) the synchronous cost is negligible, so the trade buys
  nothing here.
- **The endpoint follows the primary.** After a failover the same URL must
  reach the new primary: a VIP, a proxy (PgBouncer/HAProxy), or a Kubernetes
  service maintained by the operator. Felix instances do not re-resolve a list
  of hosts or elect anything; they reconnect to the URL they were given.
- **Backups are of the whole database, restored as a whole.** Metadata tables
  reference each other (assignments to streams and tenants, RBAC rows to
  tenants) and the `_sqlx_migrations` ledger records which schema the data is
  in. A partial restore, or a restore without that ledger, leaves instances
  refusing readiness ("no migrations applied") or misreading rows. Use
  physical backups or full-database logical dumps; point-in-time recovery is
  fine because any consistent point is a state Felix has actually been in.

## Supported topologies

Either of these satisfies the contract; pick by what you already operate.

- **Managed Postgres** (RDS/Aurora, Cloud SQL, Azure Database, …) with
  multi-AZ/HA enabled. The provider owns replication, failover, and the stable
  endpoint; verify the offering's failover is synchronous (or "zero data loss")
  rather than async replica promotion.
- **Operator-managed Postgres on Kubernetes**: CloudNativePG or
  Patroni-based operators (Zalando, Crunchy). Configure at least one
  synchronous standby and point Felix at the operator's *read-write* service
  (e.g. CloudNativePG's `<cluster>-rw`), which is exactly the
  endpoint-follows-primary requirement.

A single Postgres with no standby is fine for development and is a conscious
availability decision in production: control-plane instances add nothing when
the one database is gone.

## What a failover looks like from Felix

No Felix-side action is required at any point in this sequence; that is the
design.

1. The primary fails. In-flight statements error; each instance's pooled
   connections start failing.
2. Within a readiness window (checks are cached for
   `FELIX_READINESS_CACHE_TTL_MS`, default 1s, and bounded by
   `FELIX_READINESS_TIMEOUT_MS`, default 2s) every instance fails
   `/v1/system/ready` and load balancers stop routing to all of them.
   **Liveness keeps passing**: this is an external outage, and restarting
   instances neither helps nor is triggered.
3. The Postgres platform promotes a standby and moves the endpoint.
4. Instances reconnect through the same URL on their next queries; the first
   readiness check that succeeds (within one cache window of connectivity
   returning) puts each instance back in rotation.
5. Brokers, meanwhile, retry heartbeats and keep their last-known catalog. They
   keep serving the shards they lead only while their lease holds, which is
   0.75 × `FELIX_NODE_EXPIRY_TIMEOUT_MS` from the last accepted heartbeat: with
   the defaults (15s expiry, 5s heartbeat interval) every broker stops serving
   the shards it leads somewhere between about 5s and 11s into the outage.
   That is deliberate; see
   [the replication design](replication-design.md#failure-model).
6. Once the database is back, each broker's next heartbeat lands within about
   3s (retries are capped at a fifth of the lease) and renews its lease, so
   service resumes without a restart. The expiry sweep does not run until it
   has watched for one full expiry window after reading the store again, so
   the outage itself cannot mark a broker down; see
   [control-plane.md](control-plane.md#broker-liveness).

The visible cost of a failover is therefore the platform's promotion time plus
a few seconds on each side: readiness lag, and the broker's next heartbeat. A
promotion shorter than about 5s is usually absorbed by the lease; a longer one
makes the shards unavailable for the rest of the outage, but does not take
brokers out of the cluster. If something does mark a broker down (for example
a network partition between brokers and the control plane while the database
stays up), the broker registers again on its next heartbeat and is placeable
again under a new incarnation.

## Sizing and connections

Each instance opens one pool of `FELIX_CONTROLPLANE_POSTGRES_MAX_CONNECTIONS`
(default 10), shared by request handlers, the readiness probe (one query per
second at most, thanks to the cache), the node-expiry sweep, and the shard
reconciler. For N instances, provision Postgres `max_connections` with
headroom:

```
N × pool size + platform overhead (replication, backups, admin) + ~20%
```

Two instances at the default pool size fit comfortably inside even the small
managed tiers. The workload is many small statements (heartbeats, changefeed
polls, CRUD), so per-connection memory matters more than parallel query
capacity, and adding control-plane instances scales *availability*, not
database throughput: the database remains the shared bottleneck, which at
these rates it is nowhere near.

If you front Postgres with PgBouncer, use session pooling (or transaction
pooling with no session state assumptions; Felix uses none, but sqlx prepared
statements require `max_prepared_statements` support in PgBouncer ≥ 1.21).

## Migrations and rolling deploys

Every instance runs the embedded migrations at startup, under sqlx's advisory
lock, so concurrent starts are safe and there is no separate migration job to
orchestrate. Migrations are additive; readiness fails only when the database
is *behind* the running build ("the database is at migration X, this build
expects Y"), never when it is ahead. During a rolling deploy the first new
instance migrates the database forward and old instances keep serving on the
now-newer schema. The consequence for operators: **roll forward, not back**:
a build older than the schema keeps working, but restoring a pre-migration
backup under a newer build makes every instance refuse readiness until the
migrations rerun, which they do on the next restart.

## Whose failure is whose

| Failure | Handled by |
| --- | --- |
| A control-plane instance dies or is deployed | Felix: the survivors serve; drain and readiness make it invisible ([control-plane.md](control-plane.md#during-a-shutdown)) |
| Transient connection loss, pool exhaustion, statement timeouts | Felix: readiness fails and recovers on its own; brokers retry |
| Primary failover, replication, endpoint movement | The Postgres platform |
| Durability of committed metadata | The Postgres platform (synchronous replication) |
| Backups, restore, point-in-time recovery | The Postgres platform, per the whole-database rule above |
| Schema migrations and cross-version readiness | Felix |

## When to reconsider Felix-owned Raft

**Reconsidered: the "should Felix run without a database platform" trigger
below fired, and the alternative now has a decided design,
[metadata-raft-design.md](metadata-raft-design.md) (milestone M13). Postgres
HA remains a supported backend and everything on this page stays true for
it; Raft is the option that removes the external dependency.**

That alternative, control-plane instances forming their own Raft group and
owning metadata directly ([the implemented design in
metadata-raft-design.md](metadata-raft-design.md)), removes the external
dependency at the cost of Felix implementing consensus, snapshot transfer,
and its own backup story. It was deferred for as long as these triggers held,
and is kept here as the record of why it was taken up:

- Operating an HA Postgres (or paying for a managed one) is acceptable for
  every environment Felix targets. The moment Felix needs to run well where no
  database platform exists (edge sites, appliances), the dependency inverts
  from convenience to burden.
- Metadata write rates stay far below where a single primary matters.
- Failover measured in single-digit seconds is fast enough for metadata. If
  shard placement ever needs sub-second failover, platform-driven promotion
  will not get there.

Until then, Postgres HA is the better trade: it is the most operationally
understood HA component that exists, and every line of consensus code Felix
does not carry is one it cannot get wrong.

## One clock, not one per instance

Node liveness is a comparison: a heartbeat records a time, and the expiry sweep
checks it against another. Those two run on different instances, so reading
each process's own `SystemTime` would make safety depend on their wall clocks
agreeing to within the margin. That is much stronger than a bound on how fast they
drift, and something a single NTP step breaks.

Both sides read `ControlPlaneStore::now_millis` instead. The Postgres backend
answers with `clock_timestamp()`, so the database is the clock and every
instance judges expiry by the same one. No clock synchronisation between
control-plane instances is assumed or required.

`clock_timestamp()` rather than `now()`: `now()` is the transaction's start
time and is identical for every call within one, which is not what a clock
means.
