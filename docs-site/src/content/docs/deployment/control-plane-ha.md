---
title: "Control-plane high availability"
description: "Run three or more control-plane instances as a Raft group: what Felix handles, what you provide, and what a failover looks like."
---

The control plane holds the cluster's metadata: tenants, streams, membership,
shard ownership and auth configuration. It runs as a Raft group. Three or more
instances each keep the metadata on their own volume and replicate it between
them, so there is no database to run. A group of three survives the loss of any
one instance. How the group works inside is on
[Metadata Raft](/architecture/metadata-raft/); this page is about running it.

High availability has these parts:

- **The instances.** Run an odd number, three or more. A group of one or two
  cannot lose an instance and keep a quorum, and an even group tolerates the
  same failures as the odd one below it. Route traffic on the readiness probe.
  A rolling restart of every member, with broker heartbeats and
  shard-assignment watches flowing throughout, serves every call.
- **Placement runs on the leader.** The expiry sweep and shard placement run
  only on the Raft leader, so there is one of each. A new leader takes over
  after an election, and every placement write is still fenced by the
  generation and token it was planned from
  ([control-plane.md](https://github.com/GetFelix/felix/blob/main/docs/control-plane.md)).
  `felix_placement_lease_held` summed across instances is 1.
- **The volumes.** Each member's data directory must survive a restart,
  because that is what makes a restart a rejoin. A member that loses its
  volume rejoins empty, does not vote until it has caught up, and is rebuilt by
  snapshot install from the others.

## What Felix asks of you

- **One volume per member, on storage that honours fsync.** The Raft log and
  snapshots live there. On Kubernetes that is a PersistentVolumeClaim per
  member; the [Helm chart](/deployment/kubernetes/) renders it.
- **Members that can reach each other on the peer port.** Every member carries
  the same peers map and the same cluster id, and the peer token (32 or more
  random characters, from a Secret) authenticates every Raft request. Expose
  the peer port to the members only, and add peer mTLS where the network is not
  yours. The settings are on [Metadata Raft](/architecture/metadata-raft/#configuring-it).
- **Spread members across failure domains** where you can. Three members in one
  zone share that zone's fate.
- **Back up the volumes, and restore them as a whole.** A broker kept running
  across a restore to an earlier point refuses a stream whose restored
  incarnation is below the one it holds, since it never moves a name back.
  Restore a backup point with the brokers stopped, as
  [Backup and restore](/deployment/backup-and-restore/#restoring) says, or
  delete and recreate the stream: the new create gets a higher incarnation.

## What a failover looks like from Felix

No Felix-side action is required at any point. That is the design.

1. The leader's instance dies. The other two stop hearing its heartbeats.
2. After an election timeout one of them wins an election. Writes pause for
   that long; reads keep serving from every member's local state, so broker
   watches carry on.
3. A write in flight can get `503 unavailable`; it may still have committed, so
   read before retrying. Brokers keep
   their last-known catalog, and keep serving the shards they lead while their
   lease holds (0.75 × `FELIX_NODE_EXPIRY_TIMEOUT_MS` from the last accepted
   heartbeat). An election is much shorter than that.
4. The new leader starts the expiry sweep and placement. The sweep waits one
   full expiry window after it can read the store, so the election itself does
   not mark brokers down.
5. The dead instance restarts, finds its data directory, and rejoins as a
   follower.

If the group loses its quorum, the survivors fail readiness rather than serve
writes that cannot commit. Brokers keep serving on their catalogs and leases,
as during any control-plane outage. A shard whose leader dies during the outage
is not replaced until the control plane is back, unless the fleet has finalized
`replica_election`: then its replicas elect a new leader themselves. See
[Failover without the control plane](/deployment/failover-without-control-plane/).

## Probes

| Path | Question | Wire it to |
| --- | --- | --- |
| `/v1/system/live` | Should this process be restarted? Touches nothing outside the process. | liveness probe |
| `/v1/system/ready` | Should this instance get traffic? A Raft member answers yes when it knows a leader and has applied its log, and a leader only while a quorum has recently acknowledged it. | readiness probe |

Recommended settings: readiness `periodSeconds: 2–5` (answers are cached 1s,
so polling faster costs nothing and gains nothing), `timeoutSeconds: 3`
(above the 2s internal bound, so the reason is reported rather than lost),
`failureThreshold: 2–3`. For liveness, `periodSeconds: 10`, `failureThreshold: 3`.
Liveness must never depend on the group. A restart is the one response a lost
quorum does not deserve, and Felix's `/v1/system/live` deliberately answers
from memory.

On SIGTERM an instance fails readiness first and keeps serving for
`FELIX_SHUTDOWN_PREDRAIN_MS` so load balancers can act on it, then drains
against `FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS`. The whole sequence is on
[Graceful Shutdown](/deployment/graceful-shutdown/).

## Whose failure is whose

| Failure | Handled by |
| --- | --- |
| An instance dies or is deployed | Felix. The group elects if needed, and drain and readiness make it invisible |
| An instance loses its volume | Felix. It rejoins empty and is rebuilt by snapshot install |
| Quorum lost | You: bring members back. Felix fails readiness and brokers ride on their leases |
| Volume durability and backups | Your storage platform |
| Spreading members across failure domains | You |

## Coming from Postgres

The Postgres metadata backend was removed. The control plane refuses to start
while `FELIX_CONTROLPLANE_POSTGRES_URL` or `FELIX_CONTROLPLANE_STORAGE_BACKEND=postgres`
is set. There is no migration path: Felix is alpha. Stand up a Raft group and
recreate your tenants, streams and caches.
