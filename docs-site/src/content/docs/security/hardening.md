---
title: "Hardening a Deployment"
description: "The production security checklist for Felix: certificates, client identity, mTLS between brokers and control-plane members, tokens, RBAC, network exposure, rotation and monitoring."
---

Work through this before a cluster takes real traffic. Each item names the
exact setting. Defaults and the full description of every variable are in the
[environment reference](/reference/environment-variables/), and the reason
each item matters is in the [threat model](/security/threat-model/).

The broader production checklist (sizing, durability, limits) is
[Running in production](/deployment/production/).

## Certificates and TLS

- [ ] **A real broker certificate.** `FELIX_TLS_CERT` and `FELIX_TLS_KEY`, with
      `FELIX_TLS_REQUIRE_CERT=true` so a broker without one refuses to start.
      The certificate must carry every name clients dial: the load-balanced
      name and each broker's advertised name. The same certificate serves the
      Kafka and MQTT listeners.
- [ ] **TLS left on for Kafka and MQTT.** Never set `FELIX_KAFKA_TLS=false` or
      `FELIX_MQTT_TLS=false` outside a laptop: the token is the password.
- [ ] **`FELIX_TLS_REQUIRE_ALPN=true`**, once every client offers the `felix/1`
      ALPN.
- [ ] **TLS on the control-plane API.** `FELIX_CONTROLPLANE_TLS_CERT` and
      `FELIX_CONTROLPLANE_TLS_KEY` on the control plane, an `https://`
      `FELIX_CONTROLPLANE_URL` on every broker, and `FELIX_CONTROLPLANE_CA`
      when the certificate comes from a private CA.
- [ ] **One CA per job.** Use separate CAs for client certificates, broker
      internal certificates, and control-plane Raft certificates. Any
      certificate from the internal CA is treated as a broker, and membership
      is not yet checked per request
      ([#2909](https://github.com/GetFelix/felix/issues/2909)), so neither the
      internal CA nor the Raft CA should issue anything else.

## Client identity

- [ ] **Client certificates where the client can present one.**
      `FELIX_TLS_CLIENT_CA` makes every QUIC, Kafka and MQTT client present a
      certificate from that CA. Only the Rust client can today; the Python,
      TypeScript, C and C# clients cannot connect to a broker with it set.
- [ ] **Subject binding.** `FELIX_TLS_CLIENT_CERT_BIND_SUBJECT=true` (needs
      `FELIX_TLS_CLIENT_CA`) refuses a token unless the client certificate
      was issued to its subject: a `felix:principal:<sub>` URI SAN, or a DNS or
      IP SAN. A gateway's delegated tokens bind to the gateway's certificate
      through their `act` claim. Set it the same on every broker: a shard's
      owner checks forwarded requests against the client's certificate too.
- [ ] **The same on the control plane.** `FELIX_CONTROLPLANE_TLS_CLIENT_CA`
      refuses callers without a certificate, and
      `FELIX_CONTROLPLANE_TLS_CLIENT_CERT_BIND_SUBJECT=true` ties each token to
      it. Brokers then present `FELIX_CONTROLPLANE_CLIENT_CERT` and
      `FELIX_CONTROLPLANE_CLIENT_KEY`. Point health probes at `/ready` and
      `/live` on `FELIX_CONTROLPLANE_METRICS_BIND`, which never ask for a
      certificate.

## Between brokers and between control-plane members

- [ ] **Broker mTLS with one certificate per broker.** `FELIX_INTERNAL_TLS_CERT`,
      `FELIX_INTERNAL_TLS_KEY` and `FELIX_INTERNAL_TLS_CA` on every broker. Each
      certificate carries its broker's `FELIX_NODE_ID` as a DNS name, which is
      checked in both directions. Never set
      `FELIX_INTERNAL_ALLOW_UNAUTHENTICATED=true` without a network policy that
      admits brokers alone to `FELIX_INTERNAL_BIND`.
- [ ] **Control-plane Raft mTLS.** `FELIX_RAFT_TLS_CERT`, `FELIX_RAFT_TLS_KEY` and
      `FELIX_RAFT_TLS_CA` on every member, with one certificate per member. Each
      names the host other members reach it by in `FELIX_RAFT_PEERS`, and
      carries the URI SAN `felix:principal:raft-member:<cluster_id>:<member_id>`,
      so a stolen key speaks only as its own member. See
      [Raft member certificates](#raft-member-certificates).
- [ ] **No peer token, or a strong one.** With Raft mTLS, `FELIX_RAFT_PEER_TOKEN`
      is optional; set alone it lets any holder speak as any member. Without
      TLS it is required: `openssl rand -hex 32`, held in a secret store. Set a
      unique `FELIX_RAFT_CLUSTER_ID`, and never `FELIX_RAFT_INSECURE_PEERS=true`.
- [ ] **The operator token kept off the cluster.** Set
      `FELIX_OPERATOR_TOKEN_SHA256` only while you run a restore import or a
      Raft floor waiver, and remove it afterwards. See
      [Operator routes](#operator-routes).

## Tokens

- [ ] **Short access tokens.** `FELIX_EXCHANGE_TOKEN_TTL_SECONDS` (900 s by
      default) bounds how long a stolen bearer token works and how long a
      revoked one survives on a broker cut off from the control plane.
- [ ] **A refresh lifetime you are comfortable with.**
      `FELIX_REFRESH_TOKEN_TTL_SECONDS` is 30 days by default.
- [ ] **Narrow tokens.** Ask token exchange for the actions and resources the
      client needs (`requested`, `resources`, or `permissions` pairs); it can
      only narrow what RBAC grants.
- [ ] **Separate audiences.** Give API callers `felix-controlplane` tokens and
      leave `FELIX_CONTROLPLANE_ACCEPT_BROKER_AUDIENCE` unset, so a broker cannot
      replay a client's token against the API.
- [ ] **Pinned identity providers.** Register each tenant's issuers with
      explicit audiences, and narrow
      `FELIX_CONTROLPLANE_OIDC_ALLOWED_ALGORITHMS` to what your provider signs
      with. Leave `FELIX_CONTROLPLANE_OIDC_ALLOW_INSECURE_HTTP` and
      `FELIX_CONTROLPLANE_OIDC_ALLOW_PRIVATE_IDP` unset.
- [ ] **Bootstrap used once, then off.** `FELIX_BOOTSTRAP_ENABLED=true` only
      while seeding a tenant, on a loopback `FELIX_BOOTSTRAP_BIND_ADDR`, with
      `FELIX_BOOTSTRAP_TLS_CERT`, `FELIX_BOOTSTRAP_TLS_KEY` and
      `FELIX_BOOTSTRAP_TLS_CLIENT_CA`. Never set `FELIX_BOOTSTRAP_DEV_TOKENS` on
      a shared control plane. See
      [the bootstrap threat model](https://github.com/GetFelix/felix/blob/main/docs/security/bootstrap.md).
- [ ] **Anonymous switches unset.** `FELIX_KAFKA_ANONYMOUS_TENANT` and
      `FELIX_MQTT_ANONYMOUS_TENANT` give a whole tenant to anyone who reaches
      the port.
- [ ] **The lease key protected, or absent.** Set
      `FELIX_LEASE_TOKEN_KEY_FILE` only if you use lease tokens. The file signs
      for every tenant; give it the same care as a signing key.

## RBAC

- [ ] **Least privilege.** Publishers get `stream.publish` on their streams,
      readers `stream.subscribe`, and `stream.manage`, `ns.manage` and
      `tenant.manage` go to few principals. `stream.manage` also allows delete
      and trim; there is no create-only grant yet
      ([#2805](https://github.com/GetFelix/felix/issues/2805)).
- [ ] **Cluster scope for operators only.** `tenant.manage:cluster:*` creates
      tenants and re-points an issuer's keys; `node.manage:cluster:*` changes
      any broker's membership. Brokers get `node.view:cluster:*` and
      `node.manage` on their own `node:{node_id}` only.
- [ ] **`token.delegate` for gateways only.** It lets the holder act as any user
      whose token was minted for it.

The model is in [Authentication and RBAC](/security/authentication/#rbac-model-casbin).

## Network exposure

| Port | Default | Who may reach it |
| --- | --- | --- |
| Client QUIC (`FELIX_QUIC_BIND`) | UDP `5000` | Client networks |
| Kafka (`FELIX_KAFKA_LISTEN`) | off | Client networks, when used |
| MQTT (`FELIX_MQTT_LISTEN`, `FELIX_MQTT_WS_LISTEN`) | off | Device networks, when used. For browsers, list origins in `FELIX_MQTT_WS_ALLOWED_ORIGINS`. |
| Broker internal (`FELIX_INTERNAL_BIND`) | UDP `5001` | Other brokers only |
| Broker metrics (`FELIX_BROKER_METRICS_BIND`) | TCP `8080` | Prometheus and operators. Plain HTTP, no authentication, names tenants and streams. |
| Control-plane API (`FELIX_CONTROLPLANE_BIND`) | TCP `8443` | Brokers, clients that exchange tokens, operators |
| Control-plane metrics (`FELIX_CONTROLPLANE_METRICS_BIND`) | TCP `8080` | Prometheus and probes |
| Control-plane Raft peer (`FELIX_RAFT_BIND_ADDR`) | no default (the Helm chart uses `8444`) | The other control-plane members only |
| Bootstrap (`FELIX_BOOTSTRAP_BIND_ADDR`) | `127.0.0.1:9095`, off | Loopback or a port-forward, only while enabled |

The Helm chart's NetworkPolicy enforces this when the cluster's network
plugin supports it; see [Kubernetes](/deployment/kubernetes/). Never put the
metrics, bootstrap or Raft peer ports behind a public load balancer.

## Disks

- [ ] **Encrypt the volumes** that hold `FELIX_DURABLE_STORAGE_DIR` on brokers and
      `FELIX_RAFT_DATA_DIR` on control-plane members. Felix does not encrypt
      at rest. The Raft store holds every tenant's signing keys
      ([#2801](https://github.com/GetFelix/felix/issues/2801)), so treat its
      volume and backups like a key store.
- [ ] **Keep secret files readable by the service user only**: private keys,
      `FELIX_NODE_TOKEN_FILE`, `FELIX_NODE_REFRESH_TOKEN_FILE`, the lease key
      file.

## Rotating credentials

| What | How | Restart? |
| --- | --- | --- |
| Broker client certificate (QUIC, Kafka, MQTT), broker internal certificate, control-plane API, bootstrap and Raft certificates | Write the new pair to temporary files and rename them over the old paths. Re-read every 30 s; the next handshake uses it, and open connections keep the one they started with. The new certificate must be valid now by the local clock (so a peer whose clock runs behind may refuse a just-issued one until it catches up; leave some slack before `notBefore`). The broker internal certificate must also verify against `FELIX_INTERNAL_TLS_CA` as both client and server. On the control plane it must also cover every DNS name and IP address the old one did, so a rotation cannot rename a listener or move it to a new IP; restart the member to change a name. One that fails a check is refused with an error log, and the current one stays. | No |
| Control-plane Raft member certificate | As above. The new certificate must also verify against `FELIX_RAFT_TLS_CA` as both client and server, name the same member (its URI SAN), and not be revoked. | No |
| Control-plane CA bundles (`FELIX_RAFT_TLS_CA`, `FELIX_BOOTSTRAP_TLS_CLIENT_CA`, `FELIX_CONTROLPLANE_TLS_CLIENT_CA`) | Write the new bundle to a temporary file and rename it over the old path. Re-read every 30 s. To move to a new root: ship a bundle with both roots to every member, wait more than 30 s after the last one has it, move every leaf, then drop the old root. A Raft bundle that verifies neither the member's own certificate nor the new one waiting on disk is refused, so dropping the old root too early leaves it in place; a bundle with only the new root, written together with a leaf under it, is taken, and put back if that leaf is refused. Members reload on their own 30 s timers, so a jump like that splits old-root and new-root members for up to 30 s and can cost quorum; prefer the two-root steps. The API and bootstrap client CAs are not checked: a bundle that leaves out the brokers' CA locks them out at once, so check it before you ship it. | No |
| Retiring a Raft member's certificate | Add its fingerprint to `FELIX_RAFT_TLS_REVOKED_FILE` on every member, by writing a temporary file and renaming it; re-read every 30 s, and requests under it are refused from then on. To empty the list on purpose, leave the line `# felix: none`: a file that lists nothing without it is taken for a half-written one and the current list stays. That guard does not catch a file cut between two entries, so the rename is what keeps a revocation from being dropped. `FELIX_RAFT_TLS_REVOKED` works too but needs a restart. | No |
| Broker's `FELIX_CONTROLPLANE_CLIENT_CERT` | Replace the files, restart one broker at a time ([#2837](https://github.com/GetFelix/felix/issues/2837)). | Yes |
| Broker CA bundles (`FELIX_TLS_CLIENT_CA`, `FELIX_INTERNAL_TLS_CA`) | Put the old and new roots in one bundle and restart everything, move every leaf to the new root, then drop the old root and restart again ([#2816](https://github.com/GetFelix/felix/issues/2816)). | Yes |
| Tenant signing key | Stage, activate, retire through `/v1/tenants/{tenant_id}/signing-keys`. See [Rotating signing keys](https://github.com/GetFelix/felix/blob/main/docs/auth.md#rotating-signing-keys). | No |
| Raft peer token | Three rolling restarts, below. | Rolling |
| Bootstrap token | Deploy with `FELIX_BOOTSTRAP_TOKEN` (new) and `FELIX_BOOTSTRAP_TOKEN_PREVIOUS` (old), move callers, then drop the previous one. | Rolling |
| Lease key | Add the new seed as the last line, restart; move it first, restart; remove the old line after `FELIX_LEASE_TOKEN_MAX_TTL_SECONDS`, restart. See [docs/auth.md](https://github.com/GetFelix/felix/blob/main/docs/auth.md#the-lease-key). | Rolling |
| Broker node token | Rewrite `FELIX_NODE_TOKEN_FILE`; it is re-read every 30 s. With `FELIX_NODE_REFRESH_TOKEN_FILE` the broker renews it itself. | No |
| A user's access | Revoke the principal (`POST /v1/tenants/{tenant_id}/refresh-tokens/revoke`) or remove the RBAC rule. Its open streams end on every broker. | No |

Short certificate lifetimes work as revocation on brokers: a client or
internal-port connection closes at its certificate's `notAfter`, and so does a
control-plane Raft peer connection, spread over the 30 s before it. The control
plane's API and bootstrap listeners do not yet close an open keep-alive
connection then ([#2899](https://github.com/GetFelix/felix/issues/2899)). Brokers and
the control plane check certificates against the CA only, with no CRL or OCSP;
`FELIX_RAFT_TLS_REVOKED` is the one exception.

**After a leaked signing key**, stage, activate and retire back to back. New
tokens stop using it at once, but a broker keeps accepting tokens it signed
until its next JWKS renewal, about 10 minutes, and a broker that cannot reach
the control plane keeps it until it can.

## Raft member certificates

With `FELIX_RAFT_TLS_CERT`, `FELIX_RAFT_TLS_KEY` and `FELIX_RAFT_TLS_CA` set,
each control-plane member's certificate says which member it is. The leaf
certificate must carry exactly one URI SAN:

```text
felix:principal:raft-member:<cluster_id>:<member_id>
```

`<cluster_id>` is `FELIX_RAFT_CLUSTER_ID` and `<member_id>` is
`FELIX_RAFT_NODE_ID`, for example `felix:principal:raft-member:prod-metadata:2`.
A request that speaks for another member (a vote, append-entries,
install-snapshot, a forming record, or a `from` on a standing request) is
refused with `403`. A certificate with no such SAN, or with two, is refused
the same way. A member also refuses to start under a certificate that names a
different member, or one that is revoked. So a stolen member key speaks only
as that member, and one member's certificate can be revoked on its own.

To revoke a certificate, put its SHA-256 fingerprint in
`FELIX_RAFT_TLS_REVOKED` on every member (comma-separated, hex, colons
allowed) and restart them. The list is read only at startup, and the restart
also closes any connection the revoked certificate already had. A member then
refuses a request carrying that certificate
(`felix_meta_raft_peer_rejected_total{reason="revoked"}`) and refuses to
connect to a server presenting it (`reason="revoked_server"`). After you
wipe a member, issue a new certificate for the new incarnation and revoke the
old one, so the old machine's key can neither call nor answer for the new
member. A member's client checks that the server's certificate names a member
of this cluster, but not yet that it names the member it dialed ([#2912](https://github.com/GetFelix/felix/issues/2912)).

With TLS set, `FELIX_RAFT_PEER_TOKEN` is optional. A group with only the
token still starts, but the token says only that the caller is some member,
so any holder can speak as any member, and the member logs a warning at
startup. Treat it as a fallback for groups you have not yet moved to
certificates.

## Operator routes

The restore import (`POST /internal/bootstrap/import-state`) and the Raft
floor waiver (`POST /internal/bootstrap/raft/waive-lost`) can replace the
whole store or accept lost data, so they do not take the bootstrap token,
which every member holds. They take the operator token in the
`X-Felix-Operator-Token` header, and members are configured with only its
SHA-256 digest, so reading a member's environment does not give it away:

```bash
token="$(openssl rand -hex 32)"           # keep it outside the cluster
printf %s "$token" | sha256sum | cut -d' ' -f1
# set the digest as FELIX_OPERATOR_TOKEN_SHA256 on each member and restart it
```

Without `FELIX_OPERATOR_TOKEN_SHA256` the routes answer `403`. Leave it unset
until you need one, and comma-separate two digests while you rotate.

The token is a bearer header, so the member you send it to sees it. Treat it
as spent once used: send it over the bootstrap listener's mTLS
(`FELIX_BOOTSTRAP_TLS_*`) or a port-forward, never in cleartext across the
pod network, and remove or replace the digest on every member as soon as the
import or waiver is done. A member that leads can still append any entry to
its own log, an import included; that is the crash-fault limit of Raft, not
something a credential closes.

Upgrading is breaking. Reissue every member's certificate with its URI SAN
first: builds before this check only verify the CA, so they accept the new
certificates. Then roll the new build one member at a time, keeping
`FELIX_RAFT_PEER_TOKEN` set until every member runs it: older members still
require the token. A member started under the wrong certificate exits at
startup, and the others keep their quorum. No metadata level changes.

Under the Helm chart the old shared certificate has no URI SAN, so a rolled
member cannot talk to the old ones and the rollout waits on its readiness.
Create the per-member Secrets first, run `helm upgrade`, then delete every
control-plane pod together so all members restart on their own certificates
(`kubectl delete pod -l app.kubernetes.io/component=controlplane`). The
control plane is unavailable until they elect a leader; brokers keep serving
on what they already know.

Each node that runs a member holds every member's certificate Secret, since
the chart projects them all into the init container. A compromised member
process sees only its own key, but a compromised node, or anyone who can
attach a debug container to the pod, can read them all.

The member does not record an incarnation in its vote record, so one
certificate per incarnation plus revocation stands in for it, and a member's
own client checks only the CA on the server certificate it connects to.

## Rotating the Raft peer token

Control-plane members on the Raft metadata backend that run without peer TLS
authenticate each other with `FELIX_RAFT_PEER_TOKEN`, and with TLS the token
is an extra check when set. Rotate it after a member machine is compromised, or on
a schedule. The rotation is three rolling restarts, one member at a time, and
the group keeps its quorum throughout.

Two optional variables make that possible. `FELIX_RAFT_PEER_TOKEN_NEXT` is the
token being rolled in and `FELIX_RAFT_PEER_TOKEN_PREVIOUS` the one being
retired. A member accepts requests carrying either of them as well as its
current token, but only ever sends `FELIX_RAFT_PEER_TOKEN`. So at every point in
the roll, the token each member sends is one that every other member accepts.

1. Make sure every member runs a release that knows these variables. An older
   one ignores `FELIX_RAFT_PEER_TOKEN_NEXT` and would refuse the new token at
   step 3. Generate the new token: `openssl rand -hex 32`.
2. On every member, set `FELIX_RAFT_PEER_TOKEN_NEXT` to the new token and
   restart, one member at a time. Wait for each member to rejoin before the
   next. Members still send the old token.
3. On every member, set `FELIX_RAFT_PEER_TOKEN` to the new token,
   `FELIX_RAFT_PEER_TOKEN_PREVIOUS` to the old one, unset
   `FELIX_RAFT_PEER_TOKEN_NEXT`, and restart, one at a time. Members now send
   the new token.
4. Watch `felix_meta_raft_peer_token_accepted_total{token="previous"}` on every
   member. Once it stops growing, nothing sends the old token. Anything still
   using it needs the new token.
5. On every member, unset `FELIX_RAFT_PEER_TOKEN_PREVIOUS` and restart, one at
   a time. The old token is now refused everywhere.

Finish each roll before starting the next step: every member restarted and
back in the group. Starting step 3 while a member is still on the old
configuration leaves a member sending the new token to one that only accepts
the old one.

| Step | Sends | Accepts |
| --- | --- | --- |
| Before | old | old |
| After step 2 | old | old, new |
| After step 3 | new | new, old |
| After step 5 | new | new |

A member that is restarted out of order still talks to the rest: during step 2
it sends the old token, which everyone accepts, and during step 3 it sends the
new one, which every member that finished step 2 accepts. Skipping step 2 is the
mistake to avoid. To back out after step 3, repeat it with the two tokens
swapped, which is safe while `FELIX_RAFT_PEER_TOKEN_PREVIOUS` is still set. A member that sends the new token to one that only knows the
old one is refused with `401`, counted in
`felix_meta_raft_peer_rejected_total{reason="token"}`, and logged as
"raft peer request with a missing or wrong peer token refused". Tokens are
never logged.

Both rotation variables must be at least 32 characters, and a member refuses to
start if either is set without `FELIX_RAFT_PEER_TOKEN`. With the Helm chart,
put the new token under the `next` key of the peer token Secret for step 2,
then `token` (new) and `previous` (old) for step 3, then remove `previous`.
After each Secret change, restart the members and wait for the roll to finish
before the next change:

```bash
kubectl rollout restart statefulset <release>-controlplane
kubectl rollout status statefulset <release>-controlplane
kubectl get pods -l app.kubernetes.io/component=controlplane
```

`rollout restart` returns at once; `rollout status` waits until every member
has restarted. Check that every pod is Ready before you edit the Secret again.
A pod that restarts on its own reads the Secret as it is then, so changing it
mid-roll can put that pod a step ahead of the others.

## What to monitor

All of these are in the [metrics reference](/reference/metrics/).

| Metric | Watch for |
| --- | --- |
| `felix_revocation_feed_last_success_seconds` | `time() -` this growing: the broker cannot see revocations. |
| `felix_jwks_oldest_age_seconds` | Well past 600: retired keys may still verify on that broker. At 86400 the tenant's tokens are refused. |
| `felix_broker_credential_expires_in_seconds` | Approaching zero: the broker's node token is about to expire and its renewals are failing. |
| `felix_controlplane_auth_rejected_total` | A jump, by `reason`: probing or a misconfigured client. |
| `felix_refresh_token_replays_total` | Any increase: a refresh token was used after rotation, usually a leak. |
| `felix_bootstrap_attempts_total` | Anything while you are not bootstrapping a tenant. |
| `felix_meta_raft_peer_rejected_total` | Any `token`, `cluster_id` or `revoked` refusals: something is calling the Raft port without the right credential. |
| `felix_broker_peer_inbound_rejected_total` | Refused handshakes on the internal port. |
| `felix_quic_auth_timeout_total`, `felix_kafka_refused_total`, `felix_mqtt_refused_total` | Connections that never authenticate, or are refused, by `reason`. |
| `felix_lease_tokens_issued_total` | An unexpected rate of lease token mints. |
| `felix_audit_records_dropped_total` | Any increase other than `reason="rate_limited"`: [audit records](/security/audit-log/) were not committed. Ship the `felix_audit` log target too. |

Felix does not export certificate expiry times. Watch them with your
certificate tooling, since a connection closes at `notAfter`.
