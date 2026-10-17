---
title: "Multi-tenant SaaS"
description: "Serve many customers from one Felix cluster with tenant scoping on every operation, narrowed tokens, per-key cache grants and a per-tenant publish cap."
---

A project-management product has 800 customer organizations, each with its own
users, its own event streams and its own cached state. It runs one cluster, not
800. The requirement is that a request from a user at one customer can never
read, write or even learn about another customer's data, that an engineer can
give a service account access to exactly one customer's cache keys, and that
one customer's import job cannot starve the rest.

## How this is usually built

Kafka has no tenancy in the data model, so the convention is a topic prefix
per tenant (`acme.orders`), ACLs with prefixed resource patterns that grant a
principal access to `acme.*`, and quotas per user or client id for bandwidth
and request rate. It works and is widely deployed. The scoping is a naming
convention that the ACLs enforce, so a mistake in a prefix rule is a leak, and
topic counts grow with tenants. Kafka's quotas are broader than what Felix
has, as they limit produce and fetch bandwidth and request time.

NATS is the strongest of the common options. An account is a separate
namespace for subjects, with its own users, JetStream storage limits and
connection limits, and accounts see each other only through explicit exports
and imports. Isolation is enforced by the server and does not depend on
naming. If hard tenant isolation inside a messaging system is the whole
requirement, NATS accounts are a strong fit.

Redis has key prefixes and ACLs that can restrict a user to keys matching a
pattern. That keeps honest clients in their lane, but Redis does not give a
tenant its own namespace, quotas or identity provider, and one tenant's
`KEYS` or large value affects the others.

In all three, the application usually does its own mapping from end user to
tenant, and its own check on every call.

## Why Felix fits

The tenant is part of every data-plane request. Each operation names its tenant
and namespace and the broker rejects ones it does not know, so there is no
unscoped call to forget a check on and no prefix rule that can leak. A token for
one tenant fails against another because the `tid` claim is checked after the
signature.

Your identity provider is the source of identity. The control plane exchanges a
user's OIDC token for a Felix token, with trust configured per tenant, and the
request can narrow the grant by action and resource. A browser can hold a token
for one stream and a few cache keys, so the application does not need its own
mapping from end user to permission on every call.

Cache grants go down to a key or key prefix and cover gets, puts, deletes,
conditional writes, counters and key watches. Redis ACLs match key patterns but
carry no tenant, identity provider or quota.

The broker checks the permissions inside the token without a call to the
control plane per request. A gateway serving many users holds one connection and
gives each user a handle with `Client::with_identity`, with that user's own
subscription cap and publish byte budget. Revoking a principal ends its open
streams and watches within about one control-plane sync interval.

A per-tenant publish cap keeps one customer's import from starving the rest.
Publishes queue per shard behind a per-tenant fair queue with a guaranteed
share, and the tenant over its rate is refused with a retryable `overloaded`
and `retry_after_ms`.

## How it works on Felix

The scope is part of the protocol. A tenant contains namespaces, and a
namespace contains streams and caches. Every data-plane operation on the wire
names its tenant and namespace, so there is no unscoped request to forget to
check. The broker rejects a tenant or namespace it does not know, and a token
for one tenant is useless against another: the `tid` claim is checked after
the signature. A customer is usually one tenant, with namespaces for
environments or teams inside it.

A user signs in with your identity provider, and the control plane trades that
OIDC token for a Felix token at `POST /v1/tenants/{tenant_id}/token/exchange`.
The trust is per tenant, with its own allowed issuers and audiences, and its
own signing keys. The request can only narrow what RBAC grants, by action,
by resource, or per action and resource pair, so a browser can hold a token
that can read one stream and write another and nothing else:

```http
POST /v1/tenants/acme/token/exchange
Authorization: Bearer <oidc_jwt>
Content-Type: application/json

{ "requested": [],
  "permissions": [
    "stream.subscribe:stream:acme/projects/board-17",
    "cache.write:cache:acme/projects/presence/user:42:*"
  ] }
```

The broker verifies the token against the tenant's published keys and checks
each request against the permissions inside it, without calling the control
plane per request. Cache access can be granted down to one key or a key
prefix, and the broker checks gets, puts, deletes, conditional writes,
counters and key watches against the key. Consumer groups can be granted
one group at a time.

An API gateway that serves many users holds one `Client` and gives each user a
handle with `Client::with_identity`. Every request on the handle is checked
against that user's token, each user gets their own subscription cap and
publish byte budget on the shared connection, and revoking a principal ends
its open streams and watches on every broker within about one control-plane
sync interval.

Subscribers can be told who wrote each event. With `publishers: true` in the
Rust `ClientConfig`, `event.publisher` is the `sub` of the token the broker
accepted the write from, so a presence or chat feed shows a verified author
without the publisher claiming it in the payload. A durable stream stores it
with the record once `FELIX_RECORD_PUBLISHERS=true` is set on a single broker,
or the `publisher_principal` fleet feature is finalized in a cluster. It
vouches for who wrote, not for what.

Streams and caches for a namespace are created in one call. The control plane's
batch endpoint (up to 256 items) creates them together, in one Postgres
transaction or one Raft log entry, and is safe to retry: items that already exist as asked come
back `unchanged`.

```http
POST /v1/tenants/acme/namespaces/projects/resources
Content-Type: application/json

{ "streams": [ { "stream": "events", "kind": "Stream", "shards": 4,
                 "replication_factor": 3,
                 "retention": { "max_age_seconds": 604800, "max_size_bytes": null },
                 "consistency": "Quorum", "delivery": "AtLeastOnce", "durable": true } ],
  "caches":  [ { "cache": "presence", "display_name": "Presence" } ] }
```

## Example

On a local cluster, with `felixctl` pointed at the control plane
([setup](/getting-started/felixctl/#try-it-against-a-local-cluster)):

```bash
felixctl tenant create acme --display-name 'Acme Corp'
felixctl namespace create projects --tenant acme
felixctl stream create events --shards 4 --replication 3 --consistency quorum
felixctl cache create presence --shards 2

felixctl rbac policy add role:reader stream:acme/projects/* stream.subscribe
felixctl rbac policy add role:user cache:acme/projects/presence/user:42:* cache.write
felixctl rbac grouping add 'group:https://idp.example#acme-staff' role:reader
```

Per-tenant publish caps are set in each broker's environment. This gives Acme
10 MiB a second and 5,000 messages a second, and leaves Beta unlimited on
bytes and at 100 messages a second:

```bash
export FELIX_TENANT_PUBLISH_QUOTAS="acme:10485760:5000,beta:0:100"
```

A publish over its tenant's rate is refused with a retryable `overloaded`
and reason `tenant_quota`, carrying `retry_after_ms`. A Kafka producer is
throttled with `throttle_time_ms`. The rest of the tenants still get in, because
publishes queue per shard behind a per-tenant fair queue with a guaranteed
share for each.

The cross-tenant case is covered by a runnable demo that starts a control
plane, a fake identity provider and a broker, then shows a `t1` token refused
on `t2` resources, and a second that changes RBAC live and watches the
broker's decision change without a restart. See [Demos](/demos/overview/).

## Compared with Kafka, NATS and Redis

Against Kafka, tenancy is in the model and not in topic names, so a prefix ACL
mistake is not a leak, and a token can be narrowed to what one browser session
needs. Kafka's quotas cover more, with bandwidth and request rate for producers
and consumers per user or client, and Felix limits only publish rate today.
Pick Kafka when per-client bandwidth quotas are the main requirement.

Against NATS, accounts are the stronger isolation unit. They carry limits on
subscriptions, JetStream storage and connections, and share data only through
exports and imports under the account owner's control. A Felix tenant is a scope
in a shared cluster, with no per-tenant limits beyond the publish rate and the
per-source-address connection cap. Felix puts streams, a cache, counters and
consumer groups behind that scope, with OIDC exchange and key-level cache
grants for a SaaS where end users hold tokens directly. Pick NATS when hard
per-tenant resource limits are the whole requirement.

Against Redis key prefixes, the tenant is part of every request and the cache
is scoped to it. Credentials come from your identity provider through token
exchange and not from static ACL users. Pick Redis when you need only a fast
cache for one trusted application.

## Try it

Start the stack from the
[Quickstart](/getting-started/quickstart/#try-it-in-one-command) (it covers Podman
too) and run these from the directory holding `compose.yaml`.

The stack's credentials belong to tenant `dev`. Ask for another tenant with the
same token:

```bash
docker compose run --rm felixctl pub orders 'hello'
docker compose run --rm felixctl --tenant acme pub orders 'hello'
```

The first publish succeeds. The second fails with `auth rejected`, because the
token was minted for `dev` and the broker will not accept it for `acme`. The [cross-tenant demo](/demos/overview/) goes further, with two
tenants of its own and RBAC changes taking effect while it runs.

## Where it stops

Per-tenant quotas are partial. The publish rate, in bytes and messages per
second, is capped per broker through an environment variable, across QUIC and
Kafka. Not built: quotas on subscriptions, cache memory or storage, and quotas
stored in the control plane rather than each broker's environment. A tenant can
subscribe to, cache and store without a bound, so a hostile or buggy tenant
can still fill disk or memory. One source address may hold only a bounded
number of connections.

Tenant isolation is enforced by the broker's checks, not by separate
processes or keys: tenants share brokers, logs on disk and the control
plane's database. Durable log segments are plaintext on disk, and there is no
per-tenant encryption or audit log. The broker sees plaintext payloads.
Publisher identity is recorded only when enabled, and the Python and
TypeScript bindings do not expose it yet. Client certificates are not
available from the Python and TypeScript bindings.

## Related

- [Security](/features/security/)
- [Multi-tenancy and isolation](/features/security/#multi-tenancy-and-isolation)
- [Token exchange](/features/security/#token-exchange-oidc--felix)
- [Control plane API](/api/control-plane-api/)
- [Acting for many users](/clients/rust/#acting-for-many-users)
- [Who published an event](/clients/rust/#who-published-an-event)
- [Demos](/demos/overview/)
