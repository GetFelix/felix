---
title: "Security Overview"
description: "What Felix protects, what it does out of the box, what a hardened deployment adds, and what is not built."
---

Felix keeps three things safe: the records it has acknowledged, the wall
between tenants, and the credentials and metadata that decide who may do
what. This page says how, by default and once you configure it, and points
to the rest of the section:

- [Threat model](/security/threat-model/): who might attack a cluster, where
  the trust boundaries are, and per boundary what stops each attack and what
  is still open.
- [Hardening a deployment](/security/hardening/): the production checklist,
  with the exact setting for each item, plus rotation and monitoring.
- [Authentication and RBAC](/security/authentication/): token exchange, Felix
  tokens, bootstrap, the RBAC model, revocation and lease tokens.
- [Audit log](/security/audit-log/): the control plane's record of who changed
  what, and what it can and cannot prove.
- [Reporting a vulnerability](/security/reporting/).

:::note[Security maturity]
Felix is pre-1.0 and has not been through an external security review. These
pages say which protections exist and which do not, and link an open issue
for each known gap.
:::

## Defaults and a hardened deployment

Out of the box, Felix is set up so a laptop works. Several defaults are not
safe for production, and the broker or control plane warns at startup about
most of them.

| Area | Default | Hardened |
| --- | --- | --- |
| Broker certificate | Self-signed `localhost` certificate, generated at every start. Clients cannot tell which broker they reached. | Your certificate (`FELIX_TLS_CERT`, `FELIX_TLS_KEY`), and `FELIX_TLS_REQUIRE_CERT=true` so a broker without one refuses to start. |
| Client identity | A bearer token. Whoever holds it is the client. | Client certificates (`FELIX_TLS_CLIENT_CA`), bound to the token's subject (`FELIX_TLS_CLIENT_CERT_BIND_SUBJECT=true`), so a stolen token is useless without the key. |
| Control-plane API | Plain HTTP. Node tokens, token exchange and tenant keys cross it in clear text. | HTTPS (`FELIX_CONTROLPLANE_TLS_CERT`, `_KEY`), optionally with client certificates (`FELIX_CONTROLPLANE_TLS_CLIENT_CA`). |
| Broker to broker | A cluster member refuses to start without peer mTLS, unless `FELIX_INTERNAL_ALLOW_UNAUTHENTICATED=true`. | mTLS with one certificate per broker, named for its node id (`FELIX_INTERNAL_TLS_CERT`, `_KEY`, `_CA`). |
| Control-plane Raft peers | A shared cluster id and peer token on every request, over plain HTTP. Any token holder can speak as any member. | mTLS with one certificate per member, bound to its member id (`FELIX_RAFT_TLS_CERT`, `_KEY`, `_CA`), and the peer port reachable from the other members only. |
| Bootstrap | Off. | Used once per tenant, on loopback with mTLS, then turned off again. |
| Kafka and MQTT | Off until `FELIX_KAFKA_LISTEN` or `FELIX_MQTT_LISTEN` is set. Then TLS by default, with a token as the password. | Leave TLS on and never set the anonymous-tenant switches. |
| Data on disk | Plaintext. | Filesystem or block encryption, which you provide. |

[Hardening a deployment](/security/hardening/) walks through every row.

## Transport security

What is encrypted, and what is authenticated, by default and with
configuration:

| Link | Default | With configuration |
| --- | --- | --- |
| Client ↔ broker (QUIC) | Encrypted (TLS 1.3). The broker serves a **self-signed `localhost` certificate generated at every start**, so clients cannot verify which broker they reached, and startup warns. | `FELIX_TLS_CERT` / `FELIX_TLS_KEY`: a real certificate. `FELIX_TLS_CLIENT_CA`: clients must also present a certificate. `FELIX_TLS_CLIENT_CERT_BIND_SUBJECT=true`: that certificate must be issued to the token's subject (a `felix:principal:<sub>` URI SAN, or a DNS or IP SAN), or to its actor (`act`) on a token the control plane delegated; a shard's owner checks this again on forwarded requests, so a client without a certificate (plain MQTT) is served only on shards its own broker owns. `FELIX_TLS_REQUIRE_CERT=true` refuses to start without a certificate. `FELIX_TLS_REQUIRE_ALPN=true` refuses clients that do not negotiate the `felix/1` ALPN. |
| Client ↔ broker (Kafka listener) | TLS with the same certificate as QUIC (`FELIX_KAFKA_TLS=false` turns it off). TLS 1.2 is also accepted, for older Kafka clients. | Same variables as QUIC, except `FELIX_TLS_REQUIRE_ALPN`. |
| Client ↔ broker (MQTT listener) | TLS with the same certificate as QUIC (`FELIX_MQTT_TLS=false` turns it off, and the broker logs that tokens then cross in clear text). TLS 1.2 is also accepted, for device firmware. | Same variables as QUIC, except `FELIX_TLS_REQUIRE_ALPN`. |
| Broker ↔ broker (internal port) | Encrypted, not authenticated. A broker with a node id **refuses to start** this way unless `FELIX_INTERNAL_ALLOW_UNAUTHENTICATED=true`. | `FELIX_INTERNAL_TLS_CERT` / `_KEY` / `_CA`: mutual TLS, the certificate's DNS name checked against the node id in both directions. |
| Broker / admin CLI ↔ control-plane API | **Plain HTTP**, and the control plane warns at startup. Node credentials, token exchange and tenant JWKS cross it. | `FELIX_CONTROLPLANE_TLS_CERT` / `_KEY` on the control plane, an `https://` `FELIX_CONTROLPLANE_URL` on brokers, and `FELIX_CONTROLPLANE_CA` when the certificate comes from a private CA. Add `FELIX_CONTROLPLANE_TLS_CLIENT_CA` to require a client certificate from every caller (brokers set `FELIX_CONTROLPLANE_CLIENT_CERT` / `_KEY`), and `FELIX_CONTROLPLANE_TLS_CLIENT_CERT_BIND_SUBJECT` to tie each token to that certificate. |
| Control-plane bootstrap listener | Plain HTTP, off by default. | `FELIX_BOOTSTRAP_TLS_*`: mutual TLS. See [Bootstrap](/security/authentication/#bootstrap-mode-day-0). |
| Control-plane Raft peer listener (Raft metadata backend only) | Plain HTTP, on its own port (`FELIX_RAFT_BIND_ADDR`), authenticated by a shared `FELIX_RAFT_CLUSTER_ID` and `FELIX_RAFT_PEER_TOKEN` checked on every request. A startup warning says the token alone lets any holder speak as any member. | `FELIX_RAFT_TLS_CERT` / `_KEY` / `_CA`: mutual TLS, with each member's certificate bound to its member id. The token becomes optional. See [Raft member certificates](/security/hardening/#raft-member-certificates). |
| Broker and control-plane metrics ports | Plain HTTP, no authentication. Metrics and health. Brokers also serve `/backup/offsets` there. | Nothing to configure. Keep them off public networks. |

So every QUIC connection is encrypted out of the box, the control-plane API
is not, and "authenticated" holds only for what you configure. Without a
configured broker certificate, a client that trusts the generated one trusts
whoever answers on that address, and hands it a bearer token. Set `FELIX_TLS_CERT`
for anything but development; the demos and the cluster harness distribute
trust for the generated certificate through `FELIX_TLS_CERT_EXPORT`.

On the internal port a certificate proves which broker is calling, not what it
may do. A broker answers nothing before an identity-checked `Hello`, and a
shard's Raft member answers a vote, append or snapshot only from a broker in
that group's own membership. So a broker holding a valid certificate still
cannot join, lead or truncate a group it is not in, including one it was
moved off.

### Certificate renewal

Every TLS listener re-reads its certificate and key every 30 seconds: the
broker's client listeners (QUIC, Kafka and MQTT), its internal port, and the
control plane's API, bootstrap and Raft listeners. The Raft peer client
re-reads its certificate too. A renewal written over the same paths
(cert-manager, a Kubernetes Secret volume) is used by the next handshake
without a restart, and open connections keep the certificate they started
with. A rotated Raft certificate must still name the same member and not be
revoked, or it is refused and the old one kept. The control plane's CA bundles
and `FELIX_RAFT_TLS_REVOKED_FILE` are re-read on the same timer. These are
still read once, at startup:

- The broker's CA bundles ([#2816](https://github.com/GetFelix/felix/issues/2816)).
- The broker's control-plane client certificate, `FELIX_CONTROLPLANE_CLIENT_CERT`
  and `_KEY` ([#2837](https://github.com/GetFelix/felix/issues/2837)).
- `FELIX_RAFT_TLS_REVOKED`, an environment variable.

### When a credential expires

An open connection does not outlive its credentials. Every stream ends when
the token it was opened with expires, and a connection with a client
certificate closes at that certificate's `notAfter`, on every broker
listener. Short certificate lifetimes therefore work as revocation: a renewed
certificate is used on the next connection, and an unrenewed one stops
working on brokers. The control plane's REST and Raft listeners do not yet
close an open connection at `notAfter`
([#2899](https://github.com/GetFelix/felix/issues/2899)).
Clients should refresh before expiry and reconnect. Internal broker-to-broker
connections close the same way when a node certificate expires, each up to 30
seconds early so they do not all drop at once. A Kafka or MQTT connection
under its anonymous-tenant development switch closes at its client
certificate's `notAfter` too; one with no certificate has nothing to end it.
Details:
[When a credential expires](https://github.com/GetFelix/felix/blob/main/docs/auth.md#when-a-credential-expires).

### Client-side verification

Client-side, certificate verification is Quinn's:

```rust
// Production: validate against the platform trust store
let quinn = quinn::ClientConfig::with_platform_verifier();
let config = ClientConfig::optimized_defaults(quinn);

// A private CA: trust its bundle, and dial with a name the certificate carries
let mut roots = rustls::RootCertStore::empty();
for cert in rustls::pki_types::CertificateDer::pem_file_iter("ca.pem")? {
    roots.add(cert?)?;
}
let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
let client = Client::connect(&seeds, "broker.example.com", ClientConfig::optimized_defaults(quinn)).await?;
```

The Python, TypeScript, C and C# clients take the same two choices: the platform
trust store, or a CA file, plus the server name to verify. None presents a
client certificate yet, so they cannot connect to a broker with
`FELIX_TLS_CLIENT_CA` set. See [Auth and TLS](/clients/auth-and-tls/) for each
client.

## Multi-tenancy and isolation

Everything is scoped `tenant → namespace → stream/cache`, and the scope is
part of every wire operation, so there is no unscoped request to forget to
check. The broker rejects unknown tenants, and a token for one tenant is
useless against another (the `tid` claim is checked after signature
verification, so a valid signature for the wrong tenant fails).

Within a tenant, namespaces are the isolation unit for environments or teams:
`acme/production/orders` and `acme/staging/orders` share nothing but a
naming convention.

Tenants share brokers, so isolation also means one tenant cannot starve the
others. A tenant's publish rate can be capped per broker, in bytes and in
messages per second (`FELIX_TENANT_PUBLISH_*` in the
[environment reference](/reference/environment-variables/#connection-limits-and-tenant-quotas)),
and one source address can hold at most `FELIX_MAX_CONNECTIONS_PER_IP` client
connections. Each subscriber has its own bounded queue, so a slow reader never
blocks a publisher. Quotas on subscriptions, cache use and storage are not
built ([#2804](https://github.com/GetFelix/felix/issues/2804) for storage),
and publish quotas live in each broker's environment rather than the control
plane.

## Not built

- **Encryption at rest.** Durable log segments and the control plane's Raft
  store are plaintext on disk. Use filesystem or block-level encryption.
- **End-to-end payload encryption.** The broker sees plaintext payloads. A
  client can encrypt its own payloads before publishing, since the broker
  treats them as opaque bytes, but Felix ships no key management for it.
- **Signed records.** Records carry checksums against corruption, not a
  producer's signature, so a reader cannot tell a record a producer wrote from
  one a compromised broker made up
  ([#2803](https://github.com/GetFelix/felix/issues/2803)).
- **Broker-to-broker authentication without certificates.** mTLS is built
  and required by default; with `FELIX_INTERNAL_ALLOW_UNAUTHENTICATED=true`
  peers encrypt but do not authenticate each other and the internal network
  is trusted.
- **Client certificates from the Python, TypeScript, C and C# clients.**
- **Quotas beyond the publish rate.** Subscriptions, cache use and storage
  are not limited per tenant.
- **Audit records of data access and broker actions.** The
  [audit log](/security/audit-log/) records control-plane changes and
  refusals. Publishes, subscribes and broker-side actions such as a trim are
  not in it.

The [threat model](/security/threat-model/) lists every other open item with
its issue.
