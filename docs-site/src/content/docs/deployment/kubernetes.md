---
title: "Kubernetes Deployment"
description: "Install the control plane and a broker cluster with the Helm chart, mint the first credential, and run the day-2 operations: rolling upgrades, scaling, replacing a volume."
---

Felix ships a Helm chart, at
[`deploy/helm/felix`](https://github.com/GetFelix/felix/tree/main/deploy/helm/felix),
that renders the control plane and a broker cluster with the shape the design
assumes: StatefulSets for stable broker identity, a volume per broker, the
probes and drain behaviour the binaries already ship, and the budgets and
policies that keep a rolling operation from taking a shard's replicas with it.
Every environment variable it wires is real and in the
[environment reference](/reference/environment-variables/). The chart
invents none.

The chart names `ghcr.io/getfelix/felix-broker` and
`ghcr.io/getfelix/felix-controlplane`, which releases publish and which pull
without credentials. Releases before 0.6.0-preview.2 are under
`ghcr.io/gabloe`, the project's previous owner, and so are their charts' image
names and signatures. The image tag defaults to the chart's `appVersion`.

On `main` that is the next version, which is published only once it is
released, so a default install from `main` can ask for an image that does not
exist yet. The chart on `main` can also set variables an older release's
binaries do not read, so pinning an older `image.tag` under it is not a match
either. Use one of these:

- **A release.** Check out the release tag and install the chart from there.
  Its `appVersion` is that release's image tag (`v0.6.0-preview.3` renders
  `ghcr.io/getfelix/felix-broker:0.6.0-preview.3`), and its templates match those
  binaries.
- **`main`.** Build both images from the same commit as the chart (the
  [Docker Compose page](/deployment/docker-compose/) has the build
  commands), push them to a registry your cluster can reach, and point
  `image.registry` at it.

**Pin by digest.** `broker.image.digest` and `controlplane.image.digest` take
precedence over the tag, and the digest is what the signature covers. The
release signs `image@digest` and never `image:tag`, because a tag can be moved
to point at something else and a signature over a tag would follow it.

Signing is keyless: cosign takes a short-lived certificate from the release
workflow's OIDC identity, so there is no key to store or rotate and the
signature names the workflow that produced the image. Verify before you pin:

```bash
cosign verify ghcr.io/getfelix/felix-broker:0.6.0-preview.3 \
  --certificate-oidc-issuer=https://token.actions.githubusercontent.com \
  --certificate-identity-regexp='^https://github.com/gabloe/felix/\.github/workflows/release\.yml@refs/tags/v'
```

## What the chart decides for you

| Concern | What renders | Why it is that way |
| --- | --- | --- |
| Broker identity | A StatefulSet whose pod name is `FELIX_NODE_ID` | The name is what the broker registers as, what shards are assigned to, and under peer mTLS the DNS name its certificate must carry. A replaced pod keeps all three and the volume behind them, so a replacement is a rejoin. |
| Addresses | Peers get `$(POD_IP):5001`; clients get `<pod>.<headless>.<ns>.svc:5000` | The broker requires an IP for `FELIX_NODE_ADVERTISE_ADDR` and re-registers a new one on its first heartbeat. Clients are told a name, which survives the pod being replaced. |
| Control plane over Postgres | A Deployment rolling with `maxUnavailable: 0` | Instances are stateless. A new one is ready before an old one goes, and readiness is what the Service routes on. Migrations are additive and run under an advisory lock, so mixed versions serve during the roll. |
| Control plane under Raft | A StatefulSet with a volume per member | Each member's id is its pod ordinal plus one, and every member is handed the same peers map, derived from the replica count. Odd, and at least three. |
| Credentials | Secret references only | The Postgres URL, the bootstrap token and the broker credential are read from Secrets you create. The chart never renders one, and never puts one in a ConfigMap. |
| Shutdown | A preStop sleep, then the drain, inside a derived grace period | The endpoints controller gets a head start before SIGTERM, and the drain budget fits before SIGKILL. An explicit grace period that is too short refuses to render. |
| Disruption | PodDisruptionBudgets | At most one broker at a time, which is what keeps a replication-factor-three shard's quorum through node maintenance. A wider budget refuses to render. |
| Placement | Anti-affinity by node, spread by zone | `soft` prefers, `hard` refuses to co-locate. |
| Broker zones | With `broker.zones`, one StatefulSet per zone, pinned to it and setting `FELIX_NODE_ZONE` | A pod cannot read its node's labels, so the chart fixes the zone per StatefulSet. See [Zones](#zones). |
| The internal port | On the headless Service only, and a NetworkPolicy admitting it from broker pods | Without peer mTLS, anything that reaches the port is a broker. With it, this is the second fence. |
| Peer mTLS | A cert-manager CSI volume per pod, or an explicit opt-out | Each broker needs a certificate issued to its own name. Brokers refuse to start with neither. See [Peer mTLS](#peer-mtls). |
| Client and API certificates | Secrets you provide, off by default | See [Clients](#clients) and [Control-plane TLS](#control-plane-tls). |

## Topology

| Component | Runs as | Listens on | Reached by |
| --- | --- | --- | --- |
| Control plane | Deployment over Postgres, or a StatefulSet of 3+ under Raft | `8443` TCP (REST API), `8080` TCP (metrics, `/ready`), `9095` TCP (bootstrap, only while enabled) | Brokers and operators on `8443`; under Raft, members reach each other on `8444`, token-authenticated, which nothing else needs |
| Postgres | Outside the chart | Whatever you run it on | The control plane only |
| Broker | StatefulSet, one volume per pod | `5000` UDP (client QUIC, `ports.listeners` consecutive ports from there), `5001` UDP (internal QUIC), `8080` TCP (metrics, `/ready`, `/replication/halted`) | Clients on `5000`; other brokers on `5001`; Prometheus on `8080` |

The `8080` metrics ports have no authentication. They expose names of tenants,
streams and brokers, so keep them reachable from Prometheus and operators only,
never from clients or the internet.

What depends on what, in the order it matters during an incident:

- **Brokers depend on the control plane to start**, not to keep serving. A
  broker seeds its catalog before it reports ready. Once running, it serves
  from the catalog it has and retries heartbeats while the control plane is
  away. A lease that cannot be renewed does lapse, so a long control-plane
  outage does end in shards stopping.
- **The control plane depends on its store.** Postgres unreachable, or under
  Raft fewer than a majority of members, means no writes: no placement, no
  moves, no failover.
- **Brokers depend on each other** for forwarding and replication. Nothing
  but brokers should reach `5001`. The NetworkPolicy and peer mTLS are what
  enforce that.
- **Storage is per broker.** A broker's volume holds the logs of every shard
  it leads or follows. Losing it is recoverable from replicas when the
  replication factor is above one, and is data loss when it is not.

Certificates: clients verify brokers with `broker.clientTls`, or else each
broker's self-generated certificate (see [Clients](#clients)). Brokers verify
each other with peer mTLS (see [Peer mTLS](#peer-mtls)). The control plane's
API is plain HTTP unless `controlplane.tls` is on (see
[Control-plane TLS](#control-plane-tls)). Load balancing:
the client Service must balance UDP, and only for the first connection.
Clients then connect to the broker that owns a shard by that broker's own
address, so every broker must be individually reachable by whoever the
clients are.

## Prerequisites

- Kubernetes 1.25 or later and Helm 3.8 or later.
- A StorageClass for broker volumes. Brokers fsync, and a network disk with
  provisioned IOPS is the usual choice (`gp3`, `pd-ssd`, `Premium_LRS`).
- **A Postgres** with one writable endpoint and synchronous replication, or
  the Raft backend. What the database must provide, and what a failover looks
  like from Felix, is on [Control-plane HA](/deployment/control-plane-ha/).
  The chart does not deploy a database.
- A CNI that enforces `NetworkPolicy`, or the policy is inert.
- For peer mTLS: cert-manager and
  [cert-manager-csi-driver](https://cert-manager.io/docs/usage/csi-driver/).

## Install

Three steps, because a broker refuses to start without a credential and the
credential comes out of the control plane's day-0 bootstrap.

### 1. The control plane, with bootstrap on

```bash
kubectl create namespace felix
kubectl -n felix create secret generic felix-postgres \
  --from-literal=url='postgres://felix:...@postgres-rw.db.svc:5432/felix'
kubectl -n felix create secret generic felix-bootstrap \
  --from-literal=token="$(openssl rand -hex 32)"

helm install felix deploy/helm/felix -n felix \
  --set controlplane.storage.postgres.existingSecret=felix-postgres \
  --set controlplane.bootstrap.enabled=true \
  --set controlplane.bootstrap.existingSecret=felix-bootstrap \
  --set broker.enabled=false
kubectl -n felix rollout status deployment/felix-controlplane
```

For the Raft backend instead of Postgres:

```bash
kubectl -n felix create secret generic felix-raft-peer \
  --from-literal=token="$(openssl rand -hex 32)"
# ...the install command above, plus:
  --set controlplane.storage.backend=raft --set controlplane.replicas=3 \
  --set controlplane.storage.raft.peerToken.existingSecret=felix-raft-peer
```

The peer token authenticates every Raft request between members, and it is
also what the `migrate import` tool needs, so treat it as the cluster-admin
credential it is. Empty members may form the group only until it has formed
once: a post-install hook Job waits for the API to be ready, then creates the
`<release>-felix-controlplane-raft-formed` ConfigMap, and every member start
after that runs with `FELIX_RAFT_INITIAL_CLUSTER_STATE=existing`. Members that
lose their volumes, even before the first upgrade, wait for the group rather
than start an empty one. The hook gives up after
`controlplane.storage.raft.bootstrapTimeoutSeconds` (270). Keep `helm
--timeout` above it.

The Raft peer port (`8444`) is admitted only from control-plane pods by the
chart's NetworkPolicy (add the migration tool's pods under
`controlplane.networkPolicy.raftPeerFrom`). Without peer TLS the peer token
crosses the pod network in cleartext. To encrypt and authenticate it, create a
Secret with a certificate for `*.<release>-felix-controlplane-headless`
(server and client usages), its key and the CA, and pass
`--set controlplane.storage.raft.tls.enabled=true
--set controlplane.storage.raft.tls.existingSecret=<secret>`. The chart mounts
it and sets `FELIX_RAFT_TLS_CERT`, `FELIX_RAFT_TLS_KEY` and `FELIX_RAFT_TLS_CA`.

The bootstrap listener is on its own ClusterIP Service, never behind the API's,
so it is reachable only through a port-forward.

### 2. Day 0: an operator tenant and the broker credential

Cluster scope (`node.view:cluster:*`, `node.manage`) cannot be granted by a
tenant admin, so it is seeded at bootstrap. Initialise a tenant with a policy
granting the broker role what a broker needs, and an operator role for
yourself:

```bash
kubectl -n felix port-forward svc/felix-controlplane-bootstrap 9095 &
curl -sS -X POST http://127.0.0.1:9095/internal/bootstrap/tenants/ops/initialize \
  -H "X-Felix-Bootstrap-Token: $(kubectl -n felix get secret felix-bootstrap -o jsonpath='{.data.token}' | base64 -d)" \
  -H 'Content-Type: application/json' -d '{
    "display_name": "Operations",
    "idp_issuers": [ { "issuer": "https://login.example.com/", "audiences": ["api://felix-controlplane"],
                       "claim_mappings": { "subject_claim": "sub", "groups_claim": "groups" } } ],
    "initial_admin_principals": ["p:alice"],
    "policies": [
      { "subject": "role:broker",   "object": "cluster:*", "action": "node.view" },
      { "subject": "role:broker",   "object": "cluster:*", "action": "node.manage" },
      { "subject": "role:operator", "object": "cluster:*", "action": "tenant.manage" },
      { "subject": "role:operator", "object": "cluster:*", "action": "node.view" },
      { "subject": "role:operator", "object": "cluster:*", "action": "node.manage" }
    ],
    "groupings": [
      { "user": "p:broker", "role": "role:broker" },
      { "user": "p:alice",  "role": "role:operator" }
    ]
  }'
```

Then exchange an IdP token for the broker principal (the
[token exchange](/features/security/#token-exchange-oidc--felix) flow)
and put the Felix token in a Secret:

```bash
kubectl -n felix create secret generic felix-broker-credential \
  --from-file=token=./felix-node-token
```

One token shared by every broker, carrying `node.manage:cluster:*`, is the
simple form. The stricter one is a token per broker carrying
`node.manage:node:felix-broker-0` and so on, so no broker can register, drain
or report for another: put each under a key named after its pod and set
`broker.credential.perBroker=true`.

Either way, a Felix token expires. The broker re-reads its token file every 30
seconds, and the kubelet rewrites a mounted Secret when the Secret changes, so
rotating the token means updating the Secret before the old token expires. No
restart is needed. Anything that can mint the token and write the Secret on a
schedule works: a CronJob, an external-secrets controller, a Vault agent.

`broker.credential.refreshTokenKey` does not work with the chart as shipped.
It points `FELIX_NODE_REFRESH_TOKEN_FILE` at a Felix refresh token in the same
Secret volume, but refreshing spends that token and the broker has to write
its replacement back to the file. The volume is read-only, so the write fails
and the broker never adopts the new access token. Leave `refreshTokenKey`
empty and rotate the access token in the Secret instead.

### 3. Brokers on, bootstrap off

```bash
helm upgrade felix deploy/helm/felix -n felix --reuse-values \
  --set broker.enabled=true \
  --set broker.credential.existingSecret=felix-broker-credential \
  --set broker.peerTls.enabled=true \
  --set controlplane.bootstrap.enabled=false
kubectl -n felix rollout status statefulset/felix-broker
```

`broker.peerTls.enabled=true` needs the peer Issuer from
[Peer mTLS](#peer-mtls). Without cert-manager, pass
`broker.peerTls.allowUnauthenticated=true` instead. Brokers refuse to start
with neither, and the chart refuses to render.

Each broker comes up, registers under its pod name, seeds the catalog from the
control plane, and only then reports ready, so it is never routed traffic for
streams it does not know yet. Verify:

```bash
kubectl -n felix get pods -l app.kubernetes.io/component=broker
kubectl -n felix exec felix-broker-0 -- wget -qO- http://127.0.0.1:8080/ready
# and the fleet as the control plane sees it, with an operator token:
curl -sS -H "Authorization: Bearer $OPERATOR_TOKEN" http://felix-controlplane.felix.svc:8443/v1/nodes
```

## Zones

Placement spreads a shard's copies across the zones brokers report in
`FELIX_NODE_ZONE`. Kubernetes knows each node's zone
(`topology.kubernetes.io/zone`), but the downward API cannot hand a node label
to a pod, so the chart does not discover it. List the zones instead:

```yaml
broker:
  replicas: 1          # per zone
  zones: [us-east-1a, us-east-1b, us-east-1c]
```

That renders one StatefulSet per zone, `<release>-felix-broker-<zone>`, each
with `replicas` brokers, a `nodeSelector` on `topology.kubernetes.io/zone`, and
`FELIX_NODE_ZONE` set to its zone. The headless Service, the client Service,
the NetworkPolicy and the PodDisruptionBudget still cover every broker, so the
budget of one broker at a time holds across zones: with each shard's copies in
different zones, one budget per zone would let a node-pool upgrade take a copy
from every zone at once.

A zone is read when the broker registers, so a broker's zone changes only
when it restarts. Adding a zone to the list adds a StatefulSet, whose brokers
placement fills like any new ones. Removing one deletes its StatefulSet, so
drain its brokers first, as for [scaling in](#scaling-out). Switching an
existing release between no zones and `zones` replaces every broker at once,
names and volumes included, which is an outage: choose at install. With
`zones` empty the chart renders what it always has and brokers report no zone.

## Clients

Clients connect to any broker first and follow discovery to the broker that
owns a shard. The `felix-broker` Service (ClusterIP by default) is that first
hop. Discovery then hands out each broker's own name,
`felix-broker-N.felix-broker-headless.felix.svc.cluster.local:5000`.

Clients from outside the cluster need two things: a way in, and an address
that resolves for them. Set `broker.clientService.type=LoadBalancer` on a
provider that balances UDP, and `broker.clientAdvertiseAddr` to what
each broker is reachable as from outside, with `$(POD_NAME)` expanded per pod
(`"$(POD_NAME).brokers.example.com:5000"`, say, with one record per broker).

Give brokers a real certificate with `broker.clientTls`: one
`kubernetes.io/tls` Secret shared by every broker, whose certificate names the
client Service and each broker's advertised name. With cert-manager:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: felix-broker-tls
spec:
  secretName: felix-broker-tls
  issuerRef: { name: felix-ca, kind: Issuer }
  dnsNames:
    - felix-broker.felix.svc.cluster.local
    - "*.felix-broker-headless.felix.svc.cluster.local"
```

```bash
helm upgrade felix deploy/helm/felix -n felix --reuse-values \
  --set broker.clientTls.enabled=true --set broker.clientTls.existingSecret=felix-broker-tls
```

Clients then trust the issuing CA and dial by one of those names. Renewals are
picked up without a restart. `broker.clientTls.clientCaKey` names a key in the
Secret holding a CA that clients must present a certificate from.

Without it, a broker generates its own self-signed certificate at every start
and exports it to `/var/lib/felix/export/broker-cert.pem`. Clients verify
against it. Copy it out with `kubectl exec felix-broker-0 -- cat ...`. Each
broker's is different and changes on restart, so this is for trying the chart
out.

## Control-plane TLS

Brokers send their node credential, and clients exchange tokens, over the
control-plane API. `controlplane.tls` serves it over TLS from a
`kubernetes.io/tls` Secret whose certificate names the API Service
(`felix-controlplane.felix.svc.cluster.local`). Brokers then use `https://` and
trust the Secret's `ca.crt` (`controlplane.tls.caKey`). It does not cover
the Raft members' peer port, which is separate and authenticated by the peer
token.

## Peer mTLS

`FELIX_INTERNAL_BIND` is the port brokers use to forward publishes and ship
replication to each other. With `FELIX_INTERNAL_TLS_CERT`, `_KEY` and `_CA`
set, every peer connection is mutually authenticated: a peer is a broker
holding a certificate the cluster's CA issued to its own node id, checked in
both directions. Without them the port is encrypted but unauthenticated and
anything that can reach it is a broker, so the broker refuses to start unless
told that is intended. The chart makes the same choice at render time: set
`broker.peerTls.enabled=true`, or `broker.peerTls.allowUnauthenticated=true` to
run on the NetworkPolicy alone.

Each broker needs a certificate issued to its own pod name, and a Secret
cannot vary per pod of one StatefulSet, so the chart uses cert-manager's CSI
driver: one certificate per pod, issued at start, renewed in place.

```bash
kubectl -n felix apply -f - <<'EOF'
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: felix-peer-ca
spec:
  ca:
    secretName: felix-peer-ca   # a CA key pair you hold; never the cluster's default CA
EOF
helm upgrade felix deploy/helm/felix -n felix --reuse-values \
  --set broker.peerTls.enabled=true --set broker.peerTls.issuerName=felix-peer-ca
```

Renewals are picked up from disk without a restart. Whichever mode, the
internal port is never on a routable Service, and the NetworkPolicy admits it
from broker pods only. [`docs/threat-model-internal.md`](https://github.com/GetFelix/felix/blob/main/docs/threat-model-internal.md)
sets out what is and is not defended in each mode.

## Operations

### Rolling upgrade

The order, and why, is on [Upgrades & Compatibility](/deployment/upgrades/):
control plane first, then brokers, clients whenever. The chart's budgets and
update strategies make `helm upgrade` do that order one pod at a time, but a
new broker image should still wait on the previous broker being back in its
replica sets:

```bash
helm upgrade felix deploy/helm/felix -n felix --reuse-values \
  --set controlplane.image.digest=sha256:... --set broker.image.digest=sha256:...
kubectl -n felix rollout status deployment/felix-controlplane
kubectl -n felix rollout status statefulset/felix-broker
# Between brokers, and after: nothing halted.
kubectl -n felix exec felix-broker-0 -- wget -qO- http://127.0.0.1:8080/replication/halted
```

`[]` is the answer to want. A release that changes `INTERNAL_VERSION` is not
rolling: scale brokers to zero, upgrade, scale back.

### Scaling out

```bash
helm upgrade felix deploy/helm/felix -n felix --reuse-values --set broker.replicas=5
```

New brokers register, and placement moves shards onto them from whichever
brokers lead more than their share, one move at a time by default. It is done
when `felix_shard_moves_waiting` is `0` and no assignment has a `successor`:

```bash
curl -s -H "Authorization: Bearer $OPERATOR_TOKEN" \
  http://felix-controlplane.felix.svc:8443/v1/shard-assignments |
  jq '[.items[] | select(.successor)] | length'
```

Scaling in removes the highest ordinals. Drain each first (`POST
/v1/nodes/{id}/drain` with an operator token), wait until no assignment names
it as leader or replica, then lower `replicas`. Lowering `replicas` without
draining is a failover per shard it leads, and a follower slot that stays
pointed at a broker that no longer exists. The budget refuses a value it
cannot keep a quorum under. What a move does, how long it takes, the exact
wait check and what to watch are on
[Adding, draining and removing brokers](/deployment/scaling/).

### Replacing a broker's volume

A broker whose volume is lost comes back empty under the same name and the
same assignments. For every shard it follows, the leader offers a log placed
at its oldest surviving offset. A replica holding nothing takes it and
replication resumes.

```bash
kubectl -n felix delete pvc data-felix-broker-2 --wait=false
kubectl -n felix delete pod felix-broker-2
```

It is done when the replacement is ready, `felix_broker_replication_lag_records`
on each leader it follows has fallen to where it was before, and
`/replication/halted` is empty on every broker:

```bash
for i in 0 1 2; do
  kubectl -n felix exec felix-broker-$i -- wget -qO- http://127.0.0.1:8080/replication/halted
done
```

Replace one volume at a time. A shard whose only copies were on volumes lost
together is lost, and with a replication factor of one that is every shard
the broker held. Take a volume snapshot first in that case.

### Control-plane instance loss and database failover

Nothing to do. Survivors serve, readiness takes a failing instance out of
rotation, and brokers keep their last-known catalog and retry heartbeats. Keep
`controlplane.liveness.nodeExpiryTimeoutMs` above a database failover plus
one heartbeat interval, so a failover alone never expires brokers that were
serving fine.

### Backups

The control plane's metadata lives in the database (back it up whole, restore
it whole) or, under Raft, in the members' volumes: snapshot those at the
storage layer, and keep the state file the group was seeded from, since
`felix-controlplane migrate import --overwrite` onto a fresh group is the
recovery beyond quorum loss (see [Metadata Raft](/architecture/metadata-raft/)).
That state file holds every tenant's signing-key seeds in plaintext, so keep it
as secret as the keys, and an export taken while metadata is being written is not
a consistent point.
Broker volumes hold the streams themselves. Volume snapshots are not a
consistent backup: nothing coordinates them with each other or with the
metadata, and a leader's copy can hold records no majority acknowledged. Take a
backup point with `felix-controlplane admin backup-point`, copy each leader's
shard directories against it, and cut them back with `felix-broker
restore-point` on restore. See [Backup and restore](/deployment/backup-and-restore/).

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| Broker pod in `CrashLoopBackOff`, log says a node id needs a credential | `broker.credential.existingSecret` is missing the key the pod reads (`tokenKey`, or the pod's name with `perBroker`). |
| Broker registers, then heartbeats are refused with 403 | The token lacks `node.manage` over `node:<pod name>` or `cluster:*`. |
| Broker never becomes ready | It cannot reach the control plane (`FELIX_CONTROLPLANE_URL`), or the token lacks `node.view:cluster:*`, so it never seeds a catalog. Check its log. |
| Control plane not ready, liveness fine | The store: Postgres unreachable, or the database is behind the build's migrations. That is readiness doing its job. |
| Raft group never forms | Fewer members than the peers map names, or the headless Service was changed. Every member must carry the same map. A log line saying a majority is empty and none holds the group means the members were started with `existing` and no data: the `-raft-formed` ConfigMap is left from an earlier release whose volumes are gone. Delete it and restart the members, or set `controlplane.storage.raft.initialClusterState=new` for one upgrade. |
| Raft members log `raft peer request ... refused` | The members disagree on the peer token or the cluster id; both must be the same on every member. |
| Leader logs `error replication to target=N` with `peer answered 404` (or another status) | Member N's peer address points at something that is not its peer listener, or member N rejects the leader (401/403). The leader retries it about once a second until it answers. |
| `helm upgrade` refused with a message about budgets, drains, or members | Deliberate. The message names the values that are wrong together. |
| PVC `Pending` | No default StorageClass, or the named one does not exist in this zone. |
| Peer mTLS pods stuck in `ContainerCreating` | cert-manager-csi-driver is not installed, or the Issuer cannot sign. `kubectl describe pod` shows the CSI error. |

For a shard that is not serving, a lagging or halted follower, or a client
that is refused, [Diagnosing a cluster](/deployment/diagnosing/) goes symptom
by symptom.

## Next Steps

- **Diagnose problems**: [Diagnosing a cluster](/deployment/diagnosing/)
- **Monitor deployment**: [Observability Guide](/features/observability/)
- **Control-plane HA**: [what the database must provide](/deployment/control-plane-ha/)
- **Graceful shutdown**: [what the probes and drain do](/deployment/graceful-shutdown/)
- **Configure fully**: [Configuration Reference](/reference/configuration/)
- **Secure deployment**: [Security Guide](/features/security/)
