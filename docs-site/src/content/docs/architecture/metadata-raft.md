---
title: "Metadata Raft"
description: "Control-plane metadata made highly available without an external database, by embedding a Raft group in the control-plane instances."
---

## What it is

Every control-plane instance embeds a Raft node; three instances form a
group. Metadata — tenants, streams, shard assignments, membership, auth
configuration — becomes a Raft-replicated state machine, persisted as a log
and snapshots on each instance's own volume. The external database
disappears; Postgres remains a fully supported backend, and
[Control-plane HA](/felix/deployment/control-plane-ha/) covers how to choose.

What that survives is tested rather than asserted: three instances under
continuous broker traffic come through rolling restarts, a leader killed with
SIGKILL, a leader frozen past several elections and then thawed, and a member
whose volume is wiped — with **zero failed calls, and every acknowledged write
present on every member afterwards**. The faults injected are the ones a single
machine can produce; multi-machine fault injection is not covered. The design
record, with the alternatives considered and the problems found while building
it, is
[`docs/metadata-raft-design.md`](https://github.com/gabloe/felix/blob/main/docs/metadata-raft-design.md).

```mermaid
flowchart LR
    B1[broker] -->|reads: served locally| F1[follower]
    B2[broker] -->|reads: served locally| F2[follower]
    B3[broker] -->|writes: forwarded| L[leader]
    F1 <-->|replication| L
    F2 <-->|replication| L
    L --- D1[(log + snapshots)]
    F1 --- D2[(log + snapshots)]
    F2 --- D3[(log + snapshots)]
```

## The load-bearing choices

- **The broker contract is frozen.** Snapshots, change feeds, heartbeats,
  leases — none change shape. The design lives entirely behind the store
  traits where the Postgres/memory split already lives, and the same
  contract test suites run against all three backends.
- **The state machine is the in-memory store.** `InMemoryStore` already
  implements every store trait; Raft puts a command log in front of it.
  Commands are API-shaped (`CreateStream`, `RegisterNode`,
  `BootstrapTenantAuth`), so every check-then-act race the Postgres backend
  closes with row locks, the log closes with total ordering.
- **Apply is deterministic.** Timestamps are stamped at propose time,
  generated values are carried in the command — three instances applying
  the same log reach byte-identical state, and a test asserts exactly that.
- **Reads stay local; writes go through the leader.** Broker watches are
  pull-based and eventually consistent by contract, so followers serve them.
  The expiry sweep and shard placement run only on the leader — one sweep
  because there is one leader.
- **Leases keep their arithmetic.** The Raft leader is the lease grantor; a
  new leader learns every outstanding grant from the log and waits out the
  same safety margin before granting again. Data-plane fencing is
  unchanged: it still rests on the assignment generation.
- **Why the per-shard-Raft rejection doesn't apply here**: the
  [replication design](https://github.com/gabloe/felix/blob/main/docs/replication-design.md)
  rejected Raft for stream payloads because Raft truncates divergent log
  suffixes and the segment store never rewrites. The metadata Raft log is a
  separate, kilobyte-scale log that never touches `felix-storage` — the
  invariant conflict simply doesn't arise.

## What operators would see

| Event | Behaviour |
| --- | --- |
| One instance of three dies | Writes pause for one election timeout; reads keep serving; no broker call fails |
| An instance loses its volume | Rejoins empty and does not vote or stand for election until it has caught up with the group, so an empty log cannot elect a member missing acknowledged writes. Caught up by snapshot install; no data surgery |
| Quorum lost | Survivors fail readiness rather than serve writes that cannot commit; brokers keep serving on their catalogs and leases, as during any control-plane outage |
| Migration from Postgres | A minutes-long metadata write freeze: import a consistent snapshot as the group's first state, repoint, verify, retire the database. Brokers tolerate the freeze by design |

Library: [openraft](https://github.com/databendlabs/openraft), pinned to the
stable 0.9 line, wrapped behind a seam so its pre-1.0 API churn stays
contained.

## Configuring it

Three environment variables select the backend, the same way a Postgres URL
selects Postgres, and three more are required with them: the peer listener,
the cluster id and the peer token. `FELIX_RAFT_INITIAL_CLUSTER_STATE` is
optional and defaults to `existing`.

```
FELIX_RAFT_NODE_ID=1
FELIX_RAFT_DATA_DIR=/var/lib/felix/raft
FELIX_RAFT_PEERS=1=cp-0:8444,2=cp-1:8444,3=cp-2:8444
FELIX_RAFT_BIND_ADDR=0.0.0.0:8444
FELIX_RAFT_CLUSTER_ID=prod-metadata
FELIX_RAFT_PEER_TOKEN=<at least 32 random characters, from a Secret>
FELIX_RAFT_INITIAL_CLUSTER_STATE=new   # first start of the cluster only
```

Every member must carry the **same** peers map (initializing two disjoint
member sets is how split brain is manufactured), and the data directory
must survive restarts — it is what makes a restart a rejoin.

The Raft RPCs are served on their own **peer listener**
(`FELIX_RAFT_BIND_ADDR`), and `FELIX_RAFT_PEERS` names each member's peer
listener, not its API port. Every peer request must name the cluster and
carry the peer token, or it is refused before any route runs. That token is
the cluster-admin credential — the `propose` route behind it can replace the
whole store — so keep it in a Secret, expose the peer port to the other
members only, and give the token to nothing but the members and the
`migrate import` tool. A member refuses to start without one unless
`FELIX_RAFT_INSECURE_PEERS=true`, which is for a throwaway local group. Add
`FELIX_RAFT_TLS_CERT`, `FELIX_RAFT_TLS_KEY` and `FELIX_RAFT_TLS_CA` for peer
mTLS on top.

`FELIX_RAFT_CLUSTER_ID` is recorded in the data dir on first start; a member
refuses to start on a data dir from another cluster. Empty members form a
new group only under `FELIX_RAFT_INITIAL_CLUSTER_STATE=new`. With the
default, `existing`, members that lost their volumes wait for the group
instead of quietly starting an empty control plane, so set `new` for the
cluster's first start and nothing else.

Upgrading an existing Raft group to a release with the peer listener is not
a rolling change: old members send unauthenticated RPCs to the API port, and
new ones only answer authenticated ones on the peer port. Restart every
member together (set `FELIX_RAFT_CLUSTER_ID` to any stable name; existing
data dirs adopt it). Metadata writes pause for the restart; brokers keep
serving. A release that adds a Raft command has its own rule, in
[the design doc](https://github.com/gabloe/felix/blob/main/docs/metadata-raft-design.md#upgrading). Writes reaching
a follower forward to the leader invisibly; the expiry sweep and shard
placement run only on the leader, confirmed by a linearizable check each
tick. A proposal that cannot commit — no leader, quorum lost — fails after a
bounded deadline (default 10s) rather than hanging, and the API answers it
`503` with code `unavailable`: retry once the group has a leader. The write
may still have committed after the deadline, so a retried create can answer
`409`.

Timings are tunable when the defaults (150ms heartbeat, 600–1200ms election
window, snapshot every 500 entries) don't fit: `FELIX_RAFT_HEARTBEAT_MS`,
`FELIX_RAFT_ELECTION_TIMEOUT_MIN_MS` / `_MAX_MS`,
`FELIX_RAFT_SNAPSHOT_LOGS_SINCE_LAST`, `FELIX_RAFT_LOGS_KEPT_BEHIND_SNAPSHOT`,
`FELIX_RAFT_WRITE_TIMEOUT_MS`. An election window at or below the heartbeat
is refused at startup — it would elect against healthy leaders.

### Probes under raft

`/v1/system/ready` answers from consensus state, all read locally (a probe
never costs a consensus round trip): the member knows a leader, its applied
state trails its own log by no more than a bound, and — when it *is* the
leader — a quorum has acknowledged it within the last 5s. That last clause
is what takes a partitioned, quorumless leader out of rotation before it
serves stale reads, and it is proven by test. `/v1/system/live` stays
process-local, exactly as before: losing quorum is not fixed by a restart.
The probe settings on [Control-plane HA](/felix/deployment/control-plane-ha/)
(intervals, thresholds) carry over unchanged.

Consensus position ships as metrics: `felix_meta_raft_term`,
`_is_leader`, `_leader_known`, `_last_log_index`, `_last_applied_index`,
`_snapshot_index` (gauges), plus `felix_meta_raft_forwarded_proposals_total`
(informational — the LB is handing writes to followers) and
`felix_meta_raft_write_timeouts_total` — the counter to alert on, because it
means no leader or no quorum. `felix_meta_raft_peer_rejected_total{reason}`
counts peer requests refused for the wrong cluster id or token.
`felix_meta_raft_deduplicated_proposals_total` counts retried writes answered
from the first attempt's result. `felix_meta_raft_unsupported_commands_total`
counts committed commands this build could not apply, which in a mixed-version
group means this member has fallen behind the leader; the upgrade runbook
watches it.

### Known fact: leader deploys pause writes for one election (pre-0.10 openraft)

openraft 0.9 has no leadership-transfer API, so a rolling deploy that
restarts the current **leader** pauses metadata writes for one election
timeout (~1.2s at defaults) while a successor elects itself. Reads keep
serving, followers restart with no pause, and brokers are unaffected by
construction — they retry heartbeats and keep their catalogs through far
longer outages than this. `transfer_leader` arrives with openraft 0.10, and
the seam owns the shutdown path, so adopting it is a contained change. Until
then: a bounded, documented fact, not a bug.

### Deploying on Kubernetes

The shape the design assumed from the start — a StatefulSet with one PVC
per member and a headless service for stable peer names:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: felix-controlplane
spec:
  serviceName: felix-controlplane      # headless: stable per-pod DNS
  replicas: 3
  template:
    spec:
      containers:
        - name: controlplane
          env:
            - name: POD_NAME
              valueFrom: { fieldRef: { fieldPath: metadata.name } }
            # Ordinal → node id (an initContainer or entrypoint derives
            # FELIX_RAFT_NODE_ID = ordinal + 1 from POD_NAME).
            - name: FELIX_RAFT_DATA_DIR
              value: /var/lib/felix/raft
            - name: FELIX_RAFT_PEERS
              value: "1=felix-controlplane-0.felix-controlplane:8444,2=felix-controlplane-1.felix-controlplane:8444,3=felix-controlplane-2.felix-controlplane:8444"
            - name: FELIX_RAFT_BIND_ADDR
              value: "0.0.0.0:8444"
            - name: FELIX_RAFT_CLUSTER_ID
              value: felix-controlplane
            - name: FELIX_RAFT_PEER_TOKEN
              valueFrom: { secretKeyRef: { name: felix-raft-peer, key: token } }
            # `new` on the cluster's first start only.
            - name: FELIX_RAFT_INITIAL_CLUSTER_STATE
              value: existing
          volumeMounts:
            - name: raft
              mountPath: /var/lib/felix/raft
          readinessProbe:
            httpGet: { path: /v1/system/ready, port: 8443 }
            periodSeconds: 2
          livenessProbe:
            httpGet: { path: /v1/system/live, port: 8443 }
            periodSeconds: 10
  volumeClaimTemplates:
    - metadata: { name: raft }
      spec:
        accessModes: ["ReadWriteOnce"]
        resources: { requests: { storage: 1Gi } }
```

The PVC is what makes a pod restart a rejoin; a member whose volume is lost
rejoins empty and is rebuilt by snapshot install. The
[Helm chart](/felix/deployment/kubernetes/) renders exactly this with
`controlplane.storage.backend=raft`, deriving each member's id from its pod
ordinal and the peers map from the replica count. It lets empty members
start with `new` only until a post-install hook has seen the group form and
recorded that in a ConfigMap; every start after that is `existing`.

## Migrating from Postgres

An offline cutover measured in minutes, which brokers tolerate by design
(they keep serving on their catalogs and leases, as during any
control-plane blip):

```
# 1. Stand up the fresh Raft group (its import guard refuses a used one).
# 2. Freeze writes: take the Postgres-backed instances out of rotation.
# 3. Export through the store traits — exactly what the API serves:
FELIX_CONTROLPLANE_POSTGRES_URL=postgres://... \
  felix-controlplane migrate export-postgres state.json

# 4. One atomic command, proposed to any member's peer listener as a peer
#    (it forwards to the leader):
FELIX_RAFT_CLUSTER_ID=prod-metadata FELIX_RAFT_PEER_TOKEN=... \
  felix-controlplane migrate import state.json http://cp-0:8444

# 5. Compare the printed summaries, spot-check, repoint, retire Postgres.
```

Each step before the repoint has a clean abort: nothing is half-migrated,
because the import is a single log entry applied atomically everywhere.
Change feeds carry their sequence high-water marks, so a broker at the head
continues without noticing and one behind the head resnapshots exactly once
— the ordinary signal it already honours.

**Disaster recovery** is the same mechanism: the export file is the DR
artifact, and `migrate import … --overwrite` onto a fresh group is the
restore. `--overwrite` discards whatever the target holds — checkpoints
included — so it belongs in a runbook, run deliberately, and nowhere else.

Two things to know before relying on an export as a backup:

- **It is not a consistent read.** The export is many separate queries, not one
  transaction, so it is a state the cluster was actually in only when metadata
  writes were frozen while it ran.
- **It is a credential.** The file holds every tenant's Ed25519 signing-key seeds
  in plaintext JSON, enough to mint tokens for any tenant. Keep it encrypted and
  access-controlled. Refresh tokens are not exported, so they do not survive a
  restore.

## How it is built

The state machine is real: `MetadataStateMachine` wraps the same in-memory
store the control plane has always had, fed by a versioned command set with
one API-shaped command per mutation — heartbeat and expiry carry their
timestamps, bootstrap carries its candidate signing keys, so nothing inside
apply reads a clock or generates a value. The determinism harness applies a
full-coverage command script to two machines and requires **byte-identical
snapshots**, so change events published in HashMap order, or anything else
that differs between members, fails the test. On a real three-node group, eight
concurrent tenant bootstraps come out with exactly one winner and three
byte-identical replicas, settled by nothing but the order the log assigned.

### The consensus core underneath

`services/felix-controlplane-service/src/raft/` is the whole openraft surface — no
consensus type escapes it. Outside the seam there are exactly two things: a
`RaftHandle` (start, initialize, write, add-learner, promote, snapshot,
status, shutdown) and an `AppStateMachine` trait whose contract is the
determinism rule above. Consensus state — log, vote, current snapshot —
lives in one crash-safe [redb](https://github.com/cberner/redb) file per
instance: an embedded ACID store was chosen over hand-rolled files because
votes and entries that get acknowledged and then lost are how one term
elects two leaders, and that plumbing is the last place to be inventive.
The store passes **openraft's own storage conformance suite** on every test
run, the same discipline as running the node/shard contract suites against
every metadata backend.

## The SWIM question, answered

Evaluated alongside this design and **rejected for now**: replacing
heartbeat-to-control-plane liveness with SWIM-style gossip membership. The
short version — the heartbeat is also the lease renewal, so liveness and
serving authority deliberately travel one channel to one authority; failover
speed is lease-bound (~1s), not liveness-bound (15s), so faster detection
buys nothing safety uses; and SWIM's constant-load advantage pays off at
hundreds of nodes, not tens. The full decision, including the asymmetric
reachability gap that peer-reachability reports would cover more cheaply and
the triggers for reopening, is in
[`docs/control-plane.md`](https://github.com/gabloe/felix/blob/main/docs/control-plane.md#why-liveness-stays-centralized-swim-considered).
