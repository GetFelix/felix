# felix

The Felix control plane and a stateful broker cluster, as one Helm release.

What it renders:

| Component | Shape | Why |
| --- | --- | --- |
| Control plane, Postgres backend | Deployment, `maxUnavailable: 0`, `maxSurge: 1` | Instances are stateless over one HA database; a new one is ready before an old one goes, and readiness is what the Service routes on. |
| Control plane, Raft backend | StatefulSet with a volume per member and a headless Service | Members have identities; the peers map is derived from the replica count and each member's id from its pod ordinal. |
| Brokers | StatefulSet with a volume each, `OrderedReady` | The pod name is the node id: what the broker registers as, what shards are assigned to, and under peer mTLS the DNS name its certificate carries. Replacing a pod keeps all of it. |
| Budgets | PodDisruptionBudgets | Voluntary disruption takes at most one broker at a time, which keeps a replication-factor-three shard's quorum. |
| Network policy | Internal port admitted from broker pods only | Without peer mTLS reachability is the boundary; with it, this is the second fence. The internal port is never on a Service. |
| Peer mTLS | A cert-manager CSI volume per pod, or an explicit opt-out | Each broker needs a certificate issued to its own name; the CSI driver is what can vary a volume per pod of one StatefulSet. Brokers refuse to start with neither, so the chart refuses to render with neither. |
| Client and control-plane TLS (optional) | Secrets you provide, mounted as directories | One certificate for every broker, and one for the control-plane API. Renewals reach the running processes without a restart. |

Secrets are referenced, never rendered: the Postgres URL, the bootstrap token
and the broker credential come from Secrets the operator creates. Value
combinations that are each fine alone and wrong together refuse to render
(see [Refusals](#refusals)).

## Install

The chart is in this repository; there is no published index yet.

```bash
helm install felix deploy/helm/felix -n felix --create-namespace \
  --set controlplane.storage.postgres.existingSecret=felix-postgres \
  --set broker.enabled=false \
  --set controlplane.bootstrap.enabled=true \
  --set controlplane.bootstrap.existingSecret=felix-bootstrap
```

Brokers start disabled because a broker refuses to start without a credential,
and the credential comes out of the control plane's day-0 bootstrap. The full
sequence, including minting it, is on the
[Kubernetes deployment page](https://getfelix.github.io/felix/deployment/kubernetes/).
Once the credential is in a Secret:

```bash
helm upgrade felix deploy/helm/felix -n felix --reuse-values \
  --set broker.enabled=true \
  --set broker.credential.existingSecret=felix-broker-credential \
  --set broker.peerTls.enabled=true \
  --set controlplane.bootstrap.enabled=false
```

Peer mTLS needs cert-manager and its CSI driver. Without them, set
`broker.peerTls.allowUnauthenticated=true` instead: the internal port is then
encrypted but unauthenticated, and the NetworkPolicy is the only boundary.

## Values

The ones that decide the shape of a release. `values.yaml` documents the rest,
and `values.schema.json` rejects a misspelt key rather than ignoring it.

| Value | Default | Meaning |
| --- | --- | --- |
| `controlplane.replicas` | `2` | Instances. Odd and at least three under `raft`; exactly one under `memory`. |
| `controlplane.storage.backend` | `postgres` | `memory`, `postgres` or `raft`. |
| `controlplane.storage.postgres.existingSecret` | — | Secret holding the connection URL under `urlKey` (`url`). Required for `postgres`. |
| `controlplane.storage.raft.volume` | `1Gi` | The volume each Raft member keeps its log and snapshots on. |
| `controlplane.storage.raft.peerToken.existingSecret` | — | Secret holding the Raft peer token (32+ characters) under `tokenKey` (`token`). Required for `raft`: whoever holds the token can replace the metadata store. |
| `controlplane.storage.raft.peerPort` | `8444` | The members' Raft peer listener, on the headless Service only. Must differ from the API port. |
| `controlplane.storage.raft.clusterId` | full name | Names the group; each member's volume records it and refuses another. |
| `controlplane.storage.raft.initialClusterState` | — | `new` lets empty members form a group, `existing` never does. Empty means `new` until the group has formed once, then `existing`: a post-install hook Job waits for the API to be ready and creates the `<fullname>-controlplane-raft-formed` ConfigMap, which members read at every start. Lost volumes then never start an empty control plane, even before the first upgrade. |
| `controlplane.storage.raft.bootstrapTimeoutSeconds` | `270` | How long that hook waits for the group to form. Keep `helm --timeout` above it. |
| `controlplane.storage.raft.tls.enabled` | `false` | Mutual TLS on the Raft peer port (`FELIX_RAFT_TLS_*`). Off, the peer token and Raft traffic cross the pod network in cleartext. |
| `controlplane.storage.raft.tls.existingSecret` | — | Secret with `tls.crt`, `tls.key` and the CA under `caKey` (`ca.crt`). One certificate for every member, with server and client usages, naming `*.<fullname>-controlplane-headless`. |
| `controlplane.networkPolicy.enabled` | `true` | Under `raft`, admits the peer port only from control-plane pods and `raftPeerFrom` (e.g. the migration tool). Other ports stay open. |
| `controlplane.storage.raft.insecurePeers` | `false` | Run Raft with no peer token. Throwaway clusters only. |
| `controlplane.bootstrap.enabled` | `false` | The day-0 listener, on its own ClusterIP Service. Turn it off after use. |
| `controlplane.bootstrap.existingSecret` | — | Secret holding the bootstrap token under `tokenKey`, and the previous one under `previousTokenKey` while rotating. |
| `controlplane.shutdown.predrainMs` / `drainTimeoutMs` | `2000` / `10000` | Readiness fails, the instance keeps serving for the predrain, then drains. The grace period is derived. |
| `controlplane.podDisruptionBudget.minAvailable` | `1` | Must be below `replicas`. |
| `broker.replicas` | `3` | Brokers; per zone when `zones` is set. |
| `broker.zones` | `[]` | One StatefulSet per zone, `<fullname>-broker-<zone>`, pinned by `nodeSelector` to `topology.kubernetes.io/zone=<zone>` and setting `FELIX_NODE_ZONE`, so placement spreads each shard's copies across zones. A pod cannot read its node's labels, so the zone is set per StatefulSet. One budget still covers every zone. Brokers read the zone when they register, so changing it takes a restart; switching an existing release to or from zones replaces every broker at once, so choose at install. |
| `broker.credential.existingSecret` | — | Secret holding the Felix token the broker presents. Required while brokers are enabled. |
| `broker.credential.perBroker` | `false` | One key per broker, named after the pod, so each carries `node.manage:node:<its id>` and nothing wider. |
| `broker.credential.refreshTokenKey` | — | An IdP refresh token for re-minting before expiry. |
| `broker.controlplaneUrl` | this release's | Where the control plane is, when it is not in this release. |
| `broker.storage.size` / `storageClassName` | `50Gi` / cluster default | The volume each broker keeps its logs on. |
| `broker.ports.client` / `internal` / `metrics` | `5000` / `5001` / `8080` | Clients; brokers to each other; probes and Prometheus. Client and internal may not share a port. |
| `broker.ports.listeners` | `1` | How many client listeners each broker binds, on consecutive ports from `client`. One socket is one QUIC endpoint driver, and that driver is a single task on one core — the per-broker throughput ceiling. Raising this claims `client` .. `client + listeners - 1`, so `internal` must move out of that range; the chart refuses to render if it does not. The Services and NetworkPolicy open the whole range. The chart always sets the count, because it has to list the ports; a broker outside the chart derives `max(1, min(cores / 2, 4))`. Up to half the pod's CPU limit is a good value. |
| `broker.clientAdvertiseAddr` | pod DNS name on the client port | What discovery hands clients for each broker. `$(POD_NAME)` and `$(POD_NAMESPACE)` expand per pod. |
| `broker.clientService.type` | `ClusterIP` | The first hop for clients. `LoadBalancer` needs a provider that balances UDP. |
| `broker.peerTls.enabled` | `false` | Mutual TLS on the internal port, issued per pod by cert-manager's CSI driver from `issuerName`/`issuerKind`. |
| `broker.peerTls.allowUnauthenticated` | `false` | Run the internal port without mTLS. One of this or `enabled` is required. |
| `broker.clientTls.enabled` / `existingSecret` | `false` / — | The certificate clients verify brokers with, from a `kubernetes.io/tls` Secret. Off, each broker generates a self-signed certificate at every start. |
| `broker.clientTls.clientCaKey` | — | A key in that Secret holding the CA every client must present a certificate from. |
| `controlplane.tls.enabled` / `existingSecret` / `caKey` | `false` / — / `ca.crt` | TLS on the control-plane API, from a `kubernetes.io/tls` Secret. Brokers then use `https://` and trust `caKey` from the same Secret. |
| `broker.shutdown.preStopSeconds` / `handoffTimeoutMs` / `drainTimeoutMs` | `15` / `30000` / `40000` | The endpoints controller's head start, then the shard handoff, then the drain. The grace period is derived; an explicit one that is too short is refused. |
| `broker.podDisruptionBudget.maxUnavailable` | `1` | Must be below `replicas`, and at most one once there are three or more. |
| `broker.antiAffinity` / `topologySpread` | `soft` / zone, `ScheduleAnyway` | `hard` refuses to co-locate; `DoNotSchedule` refuses to skew. |
| `broker.networkPolicy.enabled` | `true` | Needs a CNI that enforces NetworkPolicy; otherwise it is inert. |
| `broker.config` | `{}` | The broker's config file, as a map. Its keys override the environment, so leave binds and identity to the chart. |
| `image.registry` / `*.image.digest` | `ghcr.io` / — | Pin by digest in production; a digest wins over a tag. |
| `serviceMonitor.enabled` | `false` | Prometheus Operator ServiceMonitors for both workloads. |

## Refusals

`helm template` fails, with the reason, when:

- `postgres` has no `existingSecret`, or `bootstrap` is on without one.
- `raft` has fewer than three members, or an even number, or no peer token Secret, or a peer port equal to the API port, or peer `tls` on without a Secret.
- `memory` has more than one replica.
- brokers are enabled with no credential Secret, or with no control plane and no `controlplaneUrl`.
- brokers are enabled with neither `peerTls.enabled` nor `peerTls.allowUnauthenticated`.
- `clientTls` or `controlplane.tls` is on without a Secret.
- the client and internal ports are the same.
- a budget would let every broker, or two replicas of one shard, go at once; or would never let a control-plane instance go.
- an explicit grace period is shorter than the preStop sleep plus the drain.
- `broker.zones` lists a zone twice, or is combined with a `broker.nodeSelector` on `topology.kubernetes.io/zone`, or a zone's StatefulSet name would be over 52 characters.
- a key is not in the schema.

`task chart:check` (`scripts/check_chart.py`) renders every value set under
`ci/`, checks the output for the properties above, and checks that each of
these refusals still refuses.

## Requirements

- Kubernetes 1.25 or later, Helm 3.8 or later.
- A StorageClass for the broker volumes, and for Raft members.
- For the Postgres backend: a database with one writable endpoint and
  synchronous replication, as [Control-plane HA](https://getfelix.github.io/felix/deployment/control-plane-ha/)
  sets out. The chart does not deploy one.
- For peer mTLS: cert-manager, cert-manager-csi-driver, and an Issuer or
  ClusterIssuer for the peer CA.
- For the NetworkPolicy to mean anything: a CNI that enforces it.
