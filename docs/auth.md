# Auth Design: OIDC Exchange + Felix Tokens

This document describes the current Felix authentication and authorization design as implemented in the control plane and the broker. It aligns with the `felix-authz` crate, Casbin RBAC model, and the token exchange flow in the control plane.

Detailed RBAC mutation/delegation rules and canonical grammar are documented in `docs/security/rbac.md`.

## Goals

- Multi-tenant authentication and authorization.
- Support multiple OIDC issuers per tenant.
- Brokers validate only Felix tokens (never upstream IdP tokens).
- Embed effective permissions in Felix tokens for fast broker-side enforcement.

## Components

- **Control plane**: Validates upstream OIDC tokens, evaluates RBAC policies, and mints Felix tokens.
- **felix-authz**: JWT mint/verify utilities and permission parsing/matching shared by control plane and broker.
- **Broker**: Verifies Felix token signature/claims and enforces permissions locally.

## Supported Identity Providers

Felix supports any OIDC-compliant IdP that exposes a JWKS endpoint (either via OIDC discovery or a direct `jwks_url`) and uses an allowed upstream OIDC signing algorithm.

Supported upstream OIDC JWT signing algorithms:
- `ES256` (default)
- `RS256`, `RS384`, `RS512`
- `PS256`, `PS384`, `PS512`

Control plane configuration:
- YAML: `oidc_allowed_algorithms: ["ES256"]`
- Env: `FELIX_CONTROLPLANE_OIDC_ALLOWED_ALGORITHMS=ES256,RS256,...`

This includes common providers like:

- Microsoft Entra ID
- Okta
- Auth0
- Google
- Apple

Other providers are supported as long as they provide standard OIDC discovery or a JWKS URL and the tokens include `iss`, `sub`, and `aud` claims.

## Token Types

### Upstream OIDC token

- Provided by an external IdP (Entra ID, Okta, Auth0, Google, Apple, etc.).
- Validated by the control plane using issuer discovery + JWKS.
- Claims used:
  - `iss` (issuer)
  - `sub` (subject)
  - optional groups/roles claim

### Felix token (JWT)

Minted by the control plane and verified by brokers.

Claims:
- `iss`: `felix-auth`
- `aud`: `felix-broker` or `felix-controlplane` — see [Audiences](#audiences)
- `sub`: `principal_id` (sha256 of `iss|sub`)
- `tid`: tenant id
- `exp`, `iat`
- `perms`: array of permission strings

Algorithm:
- **EdDSA (Ed25519)** (tenant-specific signing key).
- Header includes `kid`.

### Audiences

A Felix token is for brokers or for the control plane's API, never both:

- `felix-broker` (the default) is what a client presents to a broker.
- `felix-controlplane` is what the API accepts: admin tooling, operators, and a
  broker's own node credential.

Brokers see every client token they are shown and keep it to forward publishes.
If the API accepted those tokens, a compromised broker could replay a tenant
admin's client token against the API. So the API refuses `felix-broker` tokens,
and a caller asks exchange or refresh for `"audience": "felix-controlplane"`
when it wants to call the API.

`FELIX_CONTROLPLANE_ACCEPT_BROKER_AUDIENCE=true` makes the API accept
broker-audience tokens as well, for the length of a migration (see
[Upgrading](#upgrading-to-scoped-groups-and-audiences)).

## Principal Normalization

- `principal_id = sha256(iss + "|" + sub)` (hex encoding).
- Groups/roles are extracted from a configured claim (optional).
- During token exchange, each extracted group is scoped by the token's issuer
  and added as a transient role link:
  - `g, <principal_id>, group:<issuer>#<group_name>, <tenant>`
  - This allows RBAC bindings like
    `g, group:https://login.example.com#g1, role:reader, <tenant>`.

## RBAC Model (Casbin)

Casbin is used with domains for tenant scoping.

- **Domain**: tenant id
- **Role link**: `g = user, role, domain`
- **Policy rule**: `p = role, domain, object, action`
- **Matcher**: `keyMatch2` for object pattern matching

Objects:
- Tenant: `tenant:{tenant_id}`
- Namespace: `namespace:{tenant_id}/{namespace}` or `namespace:{tenant_id}/*`
- Stream: `stream:{tenant_id}/{namespace}/{stream}`, `stream:{tenant_id}/{namespace}/*`, or `stream:{tenant_id}/*/*`
- Cache: `cache:{tenant_id}/{namespace}/{cache}`, `cache:{tenant_id}/{namespace}/*`, or `cache:{tenant_id}/*/*`
- Cluster: `cluster:*` — see [Cluster scope](#cluster-scope)
- Node: `node:{node_id}` — one broker

Actions:
- `rbac.view`, `rbac.policy.manage`, `rbac.assignment.manage`
- `tenant.manage` — over `tenant:{tenant_id}` for the tenant's own settings, or
  over `cluster:*` for the tenant catalog (create, list, delete)
- `ns.manage`, `stream.manage`, `cache.manage` — the control plane's resource
  API, over the namespace, stream or cache being changed
- `stream.publish`, `stream.subscribe`
- `cache.read`, `cache.write`
- `group.consume`, `group.manage` — consumer groups, over the stream's object
  or one group's `group:{tenant_id}/{namespace}/{stream}/{group}`
- `node.view` — cluster-scoped only
- `node.manage` — over `node:{node_id}` or `cluster:*`

Consumer-group operations are split in two, over the stream's object:

- **`group.consume`**: poll, acknowledge, hand back, and list dead letters.
  `stream.subscribe` also grants it, so a reader works a group without a new
  grant.
- **`group.manage`**: redrive or discard a dead letter. `stream.manage` also
  grants it; `stream.subscribe` does not. Both change what every consumer of the
  group sees, so a principal that may only read cannot do them.

A broker that predates these actions refuses a token carrying either one, so
upgrade brokers before writing policies that use them.

Granting either action on a `group:` object scopes a principal to those groups:
its stream grants then stop covering the stream's other groups for that action.
The exact rule is in [security/rbac.md](security/rbac.md#granting-one-group).

### Cluster scope

One object sits outside the tenant hierarchy:

- Cluster: `cluster:*` — broker membership, liveness, and placement standing;
  the tenant catalog; and the metadata feeds brokers seed from.

It is an island in both directions, and that is the whole security property:

- **No tenant scope contains it.** A policy write is admitted only when its
  object is already inside the caller's scope, so a tenant admin cannot grant
  themselves `cluster:*`. Nothing in the bootstrap seed grants it either.
- **It contains no tenant object.** Cluster scope confers nothing inside a
  tenant, so it is not a backdoor into tenant data.

`node.view:cluster:*` is required by `GET /v1/nodes`, `GET /v1/nodes/{node_id}`,
and every `snapshot` and `changes` feed; `tenant.manage:cluster:*` by
`GET`/`POST /v1/tenants` and `DELETE /v1/tenants/{tenant_id}`. The tenant comes
from the token's own `tid` claim rather than a path segment, because the
cluster is not a tenant resource; that claim only selects which tenant's
signing keys to verify against, exactly as `kid` selects a key without
conferring one.

Cluster scope reaches a token only through bootstrap: initialize an operator
tenant with policies granting the cluster actions to a role, assign the
operator principal to it, and exchange. A tenant admin cannot write those rules
later, for the reason above. The full per-endpoint table is in
[control-plane.md](control-plane.md#metadata-api-authorization).

### RBAC Security Model

- RBAC mutation is **not** authorized by resource-management actions.
  - Example: `ns.manage` does not permit policy/grouping writes.
- RBAC APIs are split by intent:
  - list policies/groupings: `rbac.view`
  - mutate policy rules: `rbac.policy.manage`
  - mutate assignments/groupings: `rbac.assignment.manage`
- Delegation is scope-bound:
  - callers can only create/delete rules within their own object scope
  - callers cannot assign a role if any role policy exceeds caller scope
- Write-time validation rejects broad or malformed wildcards (for example `tenant:*`).

### Inheritance Expansion

When building effective permissions for a principal:
- `tenant.manage:tenant:{T}` implies tenant-scoped namespace/stream/cache manage + read/write actions.
- `ns.manage:namespace:{T}/{X}` implies stream/cache manage + read/write within namespace `{X}`.

### Group-Based Role Assignment (from IdP `groups_claim`)

You can assign roles to IdP groups without per-user RBAC bindings:

- Add RBAC grouping rules that bind `group:<name>` to a role.
- Configure `groups_claim` for the tenant IdP issuer.
- At token exchange time, Felix maps each incoming group value to
  `group:<issuer>#<name>`, where `<issuer>` is the validated token's `iss`
  exactly as configured, and evaluates RBAC through that link. The prefix is
  always added, so an IdP group literally named `group:ops` cannot stand in for
  a group named `ops`.

The issuer is part of the name because a tenant can trust several IdPs, and
adding one needs only `tenant.manage`. Without it, anyone who could register an
IdP could assert `groups: ["ops"]` and inherit every grant made to the
operators' `ops` group, cluster scope included. An issuer containing `#` is
refused, so the name splits unambiguously at the first `#`.

A refresh re-applies only groups whose issuer the tenant still trusts, so
removing an IdP removes its groups at the next refresh.

Example:
- `g, group:https://login.example.com#g1, role:reader, tenant-a`
- `p, role:reader, tenant-a, stream:tenant-a/payments/*, stream.subscribe`

## Control Plane Token Exchange Flow

1) Client obtains an upstream OIDC JWT from its IdP.
2) Client calls:
   - `POST /v1/tenants/{tenant_id}/token/exchange`
   - `Authorization: Bearer <oidc_jwt>`
3) Control plane validates:
   - `iss` matches a tenant-allowed issuer
   - signature using JWKS (cached with TTL; see below for unknown `kid`s)
   - `exp/nbf` with clock skew
   - `aud` matches configured audiences
4) Control plane derives `principal_id` and loads RBAC policies/groupings for the tenant.
5) Effective permissions are computed from Casbin and expanded for inheritance.
6) Optional request filters reduce the permission set. `requested` keeps only
   the listed actions. `resources` narrows each grant to the part a resource
   covers: asking for `stream:t1/payments/orders` out of a
   `stream:t1/payments/*` grant yields the one stream. A resource outside every
   grant, or one that is not an RBAC object, yields nothing; the filter never
   widens.
7) A Felix access token is minted for the requested `audience`, along with a
   refresh token, and both are returned.

If no permissions remain, the exchange returns `403`.

A token whose `kid` is not in the cached JWKS triggers a re-fetch, since that
usually means the IdP rotated keys. That lookup happens before the signature
is checked, so anyone who knows a tenant's issuer can reach it with a forged
token. Each JWKS URL is therefore re-fetched at most once every 30 seconds;
concurrent misses wait for the one fetch in progress, and a miss inside the
window is refused without contacting the IdP. A key the IdP rotates in is
accepted within 30 seconds of its first use. Fetches time out after 10 seconds.

## Staying authenticated: refresh

The access token is deliberately short-lived (900s), because a leaked bearer
token is only as dangerous as the time it stays valid. That is the right trade
for a caller that can re-exchange freely and the wrong one for anything
long-running — a broker holding a 900s node token drops out of the cluster in
fifteen minutes. Raising the TTL trades the problem for a longer-lived secret.

Refresh is the answer instead. Exchange hands back a refresh token; presenting
it to `POST /v1/tenants/{tenant_id}/token/refresh` mints a new access token
with no upstream IdP round trip.

Three properties make that safe:

- **The secret is never stored.** The control plane keeps a SHA-256 of it, so a
  database read yields nothing presentable.
- **Every refresh token is single-use.** Refreshing spends the one presented and
  hands back its replacement. A stolen copy is worth one use, not a month of
  them.
- **A replay revokes the whole chain.** Nobody legitimately presents a spent
  token, so seeing one means two parties hold the chain. Both lose it, and the
  genuine holder re-exchanges. This is what makes single use *detect* theft
  rather than merely limit it.

Permissions are re-evaluated against current RBAC on every refresh, never
carried over from the previous token. A grant removed after a token was issued
stops working at the next refresh rather than whenever the caller happens to
re-exchange — a refresh that froze its grants would turn the short access TTL
into a long one for authorization purposes, which is most of what the short TTL
was for. If every grant is gone, the refresh is refused **and the chain ends**,
so a principal whose access was removed cannot keep rotating.

The group claims presented at exchange are recorded on the refresh token,
because group-derived grants cannot be recomputed without them. They are claims
to re-check, not permissions to reuse.

So is the exchange's narrowing: its `requested` actions, `resources` hints and
`audience`. A refresh applies the same narrowing to current RBAC, so a narrowed
token's refresh token never yields more than the narrowed token did, and the
chain keeps its audience for life. Asking a refresh for a different audience is
a `400`; exchange again for the other one. A refresh token issued before the
narrowing was recorded refreshes to the principal's full RBAC rights, as it
always did.

`FELIX_REFRESH_TOKEN_TTL_SECONDS` (default 30 days) bounds a refresh token that
is stolen and *never used* — one that is used produces a replay, which ends the
chain immediately.

To cut off a principal without waiting out any token's expiry, revoke its
refresh tokens for the tenant; every chain it holds ends at once:

```
POST /v1/tenants/{tenant_id}/refresh-tokens/revoke
Authorization: Bearer <felix-controlplane token with tenant.manage:tenant:{tenant_id}>
Content-Type: application/json

{ "principal_id": "<the principal's sub>" }
```

The answer is `{"revoked": <n>}`, the number of live tokens it ended. Access
tokens already minted stay valid until they expire, which is what the short
access TTL bounds. Removing the principal's groupings stops the next refresh
too, and ends its chain.

### In the Rust client

Each client stream authenticates when it opens, and clients open streams long
after connecting: subscriptions, cache watches, group requests, and
`ClusterClient` reconnects. A fixed `ClientConfig::auth_token` stops working for
those once it expires, so long-running clients should set
`ClientConfig::token_provider` instead. The client asks it for a token every
time a stream authenticates.

`RefreshingToken` is a provider that takes a function to fetch a token
(normally the `/token/refresh` call above). It fetches a new one when two
thirds of the current token's lifetime has passed, based on `exp`. If a fetch
fails it keeps using the current token until that expires. If the broker
refuses a token, the client asks for a new one and retries once.

```rust,ignore
config.token_provider = Some(Arc::new(RefreshingToken::with_initial(
    access_token,
    move || refresh_with_control_plane(http.clone(), refresh_chain.clone()),
)));
```

The broker only checks the token when a stream opens, so a stream that is
already open keeps working after the token expires. Forwarded publishes are the
exception. When a broker forwards a publish to the shard owner, the owner checks
the token the publish stream opened with and rejects it once it has expired.

## Bootstrap Mode (Day-0)

Felix includes a **one-time operator bootstrap** flow to initialize tenant auth before any admin tokens exist.

Why it exists:
- Admin endpoints require a Felix token with explicit management permissions.
- But Felix tokens require IdP issuer configuration and signing keys.
- Bootstrap fills that gap once per tenant, then is disabled.

### How it works

1) Operator enables bootstrap on the control plane:
   - `FELIX_BOOTSTRAP_ENABLED=true`
   - `FELIX_BOOTSTRAP_BIND_ADDR=127.0.0.1:9095` (or cluster-internal address)
   - `FELIX_BOOTSTRAP_TOKEN=<random secret>`
   - optionally `FELIX_BOOTSTRAP_TOKEN_PREVIOUS=<the token being rotated out>`,
     accepted alongside the current one so a rotation is a rolling deploy
     rather than an outage
   - optionally `FELIX_BOOTSTRAP_TLS_CERT` / `FELIX_BOOTSTRAP_TLS_KEY` /
     `FELIX_BOOTSTRAP_TLS_CLIENT_CA` (all three), which makes the bootstrap
     listener terminate mTLS and refuse, at the handshake, any client without
     a certificate signed by that CA
2) Operator calls the internal endpoint:
   - `POST /internal/bootstrap/tenants/{tenant_id}/initialize`
   - header `X-Felix-Bootstrap-Token: <token>`
3) Control plane:
   - creates the tenant if missing
   - generates signing keys (EdDSA / Ed25519)
   - seeds IdP issuers
   - seeds RBAC policies + groupings
   - marks the tenant as bootstrapped
   All of it commits as one atomic store operation, exactly once per tenant:
   racing the call against itself — including through different control-plane
   instances behind one load balancer — produces one winner and `409` for
   everyone else, and a failure part-way leaves the tenant retryable rather
   than half-initialized.
4) Operator disables bootstrap after use.

The threat model, token lifetime and replay rules, the rotation procedure, and
recovery steps are in [security/bootstrap.md](security/bootstrap.md).

### Example

```
POST /internal/bootstrap/tenants/t1/initialize
X-Felix-Bootstrap-Token: <secret>
Content-Type: application/json

{
  "display_name": "Tenant One",
  "idp_issuers": [
    {
      "issuer": "https://login.microsoftonline.com/<tenant>/v2.0",
      "audiences": ["api://felix-controlplane"],
      "discovery_url": null,
      "jwks_url": null,
      "claim_mappings": {
        "subject_claim": "sub",
        "groups_claim": "groups"
      }
    }
  ],
  "initial_admin_principals": ["p:alice"]
}
```

### After bootstrap

Auth admin permissions:
- IdP issuer, signing-key and refresh-token revocation endpoints require
  `tenant.manage`.
- RBAC policy/grouping endpoints (add and remove) allow either:
  - `rbac.policy.manage` / `rbac.assignment.manage` at tenant scope (`tenant:{tenant_id}`), or
  - those same actions at narrower namespace/stream/cache scopes.
Bootstrap tokens **never** authorize normal admin endpoints.

## Broker Validation Flow

1) Client connects to a broker with `tenant_id` and a Felix token.
2) Broker verifies the token using tenant JWKS fetched from:
   - `GET /v1/tenants/{tenant_id}/.well-known/jwks.json`

   The tenant id comes from a client that has not authenticated yet, so the
   fetch is guarded. Once the broker's catalog sync has landed, a tenant it
   does not know is refused without a request. The id is percent-encoded as one
   path segment. Concurrent misses for one tenant share a single request, a
   failed fetch is remembered for 5 seconds, at most 8 fetches run at once, and
   each has a 5 second timeout. A fetched JWKS is cached for an hour.
3) Broker validates claims:
   - `iss = felix-auth`
   - `aud = felix-broker`
   - `exp/nbf`
   - `tid` matches connection tenant
4) Broker parses `perms` once at connect time and enforces per operation using keyMatch2.

## Data Stored Per Tenant

In the control plane store (memory or Postgres):
- Allowed OIDC issuers + audiences + claim mappings
- Casbin policies and groupings
- Tenant signing keys: a current key that signs, plus any keys that only
  verify, all published in the tenant JWKS and all tried on verification. See
  [Rotating signing keys](#rotating-signing-keys).

## Rotating signing keys

Every call requires a `felix-controlplane` token with `tenant.manage` over the
tenant. Responses name keys by `kid`; key material never leaves the store.

1. **Stage** a new key: `POST /v1/tenants/{tenant_id}/signing-keys`. The
   control plane generates it, publishes it in the JWKS and accepts tokens it
   signs, but keeps signing with the current key.
2. **Wait for brokers to learn it.** A broker caches a tenant's JWKS for up to
   an hour and does not refetch on an unknown `kid`, so a token signed by a key
   it has not seen is refused until its cache expires. Wait at least an hour.
3. **Activate** it: `POST /v1/tenants/{tenant_id}/signing-keys/{kid}/activate`.
   It signs from now on; the key it replaced keeps verifying.
4. **Wait for the old key's tokens to expire**: the access-token TTL
   (`FELIX_EXCHANGE_TOKEN_TTL_SECONDS`, 900 s by default) plus a minute of
   clock skew.
5. **Retire** the old key: `DELETE /v1/tenants/{tenant_id}/signing-keys/{kid}`.
   It leaves the JWKS; tokens it signed stop verifying. The current key cannot
   be retired (`409`).

`GET /v1/tenants/{tenant_id}/signing-keys` lists `current` and the keys that
only verify. Refresh tokens are not signed and are unaffected. To respond to a
leaked key, do the same with no waits, and accept that clients holding tokens
from the old key re-authenticate.

## Allowing Particular Upstream IdPs

IdP trust is **configured per tenant**. The control plane only accepts tokens from issuers listed for that tenant, and only for configured audiences.

### Admin API (preferred)

IdP issuer endpoints require a Felix token with `tenant.manage` for the tenant.
Changing an existing issuer's `jwks_url` or `discovery_url` additionally
requires `tenant.manage:cluster:*`. Principal ids are
`sha256(iss|sub)`, so whoever controls an issuer's keys can mint tokens as any
of its subjects, including ones holding cluster grants.
RBAC endpoints require explicit RBAC actions (`rbac.view`, `rbac.policy.manage`, `rbac.assignment.manage`).

Use the control plane admin endpoints to manage IdP issuers per tenant:

```
POST /v1/tenants/{tenant_id}/idp-issuers
Content-Type: application/json

{
  "issuer": "https://login.microsoftonline.com/<tenant>/v2.0",
  "audiences": ["api://felix-controlplane"],
  "discovery_url": null,
  "jwks_url": null,
  "claim_mappings": {
    "subject_claim": "sub",
    "groups_claim": "groups"
  }
}
```

Delete an issuer:

```
DELETE /v1/tenants/{tenant_id}/idp-issuers/{issuer}
```

### Postgres (control plane store)

Insert or update the issuer in the `idp_issuers` table:

```sql
INSERT INTO idp_issuers (
  tenant_id,
  issuer,
  audiences,
  discovery_url,
  jwks_url,
  subject_claim,
  groups_claim
) VALUES (
  't1',
  'https://login.microsoftonline.com/<tenant>/v2.0',
  '["api://felix-controlplane"]'::jsonb,
  NULL,
  NULL,
  'sub',
  'groups'
)
ON CONFLICT (tenant_id, issuer) DO UPDATE SET
  audiences = EXCLUDED.audiences,
  discovery_url = EXCLUDED.discovery_url,
  jwks_url = EXCLUDED.jwks_url,
  subject_claim = EXCLUDED.subject_claim,
  groups_claim = EXCLUDED.groups_claim;
```

Notes:
- If `discovery_url` is `NULL`, the control plane uses `{iss}/.well-known/openid-configuration`.
- `jwks_url` can be set directly if your IdP doesn’t support discovery.
- `subject_claim` defaults to `sub`. `groups_claim` is optional.
- Discovery and JWKS URLs must be `https`. Plain `http` is accepted only for a
  loopback host (a local IdP in development), or anywhere when
  `FELIX_CONTROLPLANE_OIDC_ALLOW_INSECURE_HTTP=true`. URLs carrying
  credentials, and other schemes, are refused. A JWKS fetched over plain HTTP
  can be replaced in transit, and whoever replaces it can mint tokens for the
  tenant. The rule is checked when an issuer is added (`400`) and again before
  every fetch, including the `jwks_uri` a discovery document returns, so an
  issuer stored before the rule existed is held to it too.
- Discovery and JWKS URLs must not point at a private (RFC 1918), link-local
  (including `169.254.169.254`), unique-local (`fc00::/7`) or unspecified
  address, whether as a literal or through what the hostname resolves to at
  fetch time. Loopback stays allowed. `FELIX_CONTROLPLANE_OIDC_ALLOW_PRIVATE_IDP=true`
  (or `FELIX_CONTROLPLANE_OIDC_ALLOW_INSECURE_HTTP=true`) lifts this for an IdP
  on an internal network. The resolver check does not apply when fetches go
  through an HTTP proxy, which resolves the name itself.
- Redirects are not followed: a discovery or JWKS URL must answer directly.
- A discovery document's `issuer` must equal the configured issuer exactly
  (OIDC Discovery 1.0, section 4.3), or the exchange fails.
- The issuer must not contain `#`; see
  [Group-Based Role Assignment](#group-based-role-assignment-from-idp-groups_claim).

### In-Memory Store (dev/tests)

Use the store API to add issuers during initialization:

```rust
store
  .upsert_idp_issuer(\"t1\", IdpIssuerConfig {
      issuer: \"https://example-idp\".to_string(),
      audiences: vec![\"felix-controlplane\".to_string()],
      discovery_url: None,
      jwks_url: Some(\"https://example-idp/.well-known/jwks.json\".to_string()),
      claim_mappings: ClaimMappings {
          subject_claim: \"sub\".to_string(),
          groups_claim: Some(\"groups\".to_string()),
      },
  })
  .await?;
```

The token exchange endpoint will reject upstream tokens if:
- the issuer is not listed for the tenant
- the audience does not match
- the signature or standard claims are invalid

## HTTP Endpoints

### Token Exchange

```
POST /v1/tenants/{tenant_id}/token/exchange
Authorization: Bearer <oidc_jwt>
Content-Type: application/json

{
  "requested": ["stream.publish", "cache.read"],
  "resources": ["namespace:t1/payments", "stream:t1/payments/orders"],
  "audience": "felix-broker"
}
```

All three fields are optional. `audience` is `felix-broker` (the default) or
`felix-controlplane`; anything else is `400`.

Response:

```
{
  "felix_token": "<jwt>",
  "expires_in": 900,
  "token_type": "Bearer",
  "refresh_token": "<token_id>.<secret>",
  "refresh_expires_in": 2592000
}
```

### Token Refresh

```
POST /v1/tenants/{tenant_id}/token/refresh
Content-Type: application/json

{
  "refresh_token": "<token_id>.<secret>",
  "audience": "felix-controlplane"
}
```

No `Authorization` header: the refresh token *is* the credential. `audience`
is optional; the new token always has the audience the exchange chose, and
naming a different one is a `400`. A broker's credential refresh asks for
`felix-controlplane`, so its refresh token must come from an exchange that asked
for `felix-controlplane` too.

Response:

```
{
  "felix_token": "<jwt>",
  "expires_in": 900,
  "token_type": "Bearer",
  "refresh_token": "<new_token_id>.<new_secret>",
  "refresh_expires_in": 2592000
}
```

**Store the replacement before using the new access token.** The token you
presented is already spent; losing the replacement means re-exchanging.

Every failure answers `403` with the same message — unparseable, unknown,
expired, revoked, wrong secret. Distinguishing them would let a caller probe
which token ids exist.

A wrong secret against a real token id also spends the token. Someone holding
half a credential gets one guess, not unlimited ones.

### Tenant JWKS

```
GET /v1/tenants/{tenant_id}/.well-known/jwks.json
```

Response:

```
{
  "keys": [
    {
      "kty": "OKP",
      "kid": "k1",
      "alg": "EdDSA",
      "use": "sig",
      "crv": "Ed25519",
      "x": "..."
    }
  ]
}
```

### RBAC rule removal

```
DELETE /v1/tenants/{tenant_id}/rbac/policies
Content-Type: application/json

{ "subject": "role:reader", "object": "stream:t1/payments/*", "action": "stream.subscribe" }
```

```
DELETE /v1/tenants/{tenant_id}/rbac/groupings
Content-Type: application/json

{ "user": "p:bob", "role": "role:reader" }
```

Both answer `204`, or `404` when no such rule exists. Removal is held to the
same scope as adding: `rbac.policy.manage` over the rule's object for a policy,
and `rbac.assignment.manage` over every policy of the role for a grouping (a
role with no policies needs tenant scope). A removed grant stops working at
the holder's next refresh, since refresh re-evaluates RBAC.

## Upgrading to scoped groups and audiences

Two changes need operator action.

**Group names.** Groupings written against bare IdP group names
(`group:ops`) stop matching, because exchange now produces
`group:<issuer>#ops`. For each such grouping, add the scoped form for every
issuer that should carry the group, then remove the bare one:

```
POST   /v1/tenants/t1/rbac/groupings  {"user": "group:https://login.example.com#ops", "role": "role:operator"}
DELETE /v1/tenants/t1/rbac/groupings  {"user": "group:ops", "role": "role:operator"}
```

`FELIX_CONTROLPLANE_LEGACY_UNSCOPED_GROUPS=true` links the bare names as well
while this is under way. It keeps the hole scoping closes open, so turn it off
once the groupings are moved. Refresh tokens issued before the upgrade record
bare names and lose their group-derived grants at their next refresh unless the
switch is on; their holders re-exchange.

**Audiences.** Every caller of the API needs a `felix-controlplane` token:
exchange with `"audience": "felix-controlplane"`. That includes each broker's
node credential (`FELIX_NODE_TOKEN` / `FELIX_NODE_TOKEN_FILE`); an
upgraded broker's refresh loop asks for the right audience itself, but the
first token has to be minted with it. To upgrade without an outage, set
`FELIX_CONTROLPLANE_ACCEPT_BROKER_AUDIENCE=true` on the control plane, upgrade,
re-provision credentials, then remove it.

Rule removal and key rotation add Raft commands. On the Raft backend, finish
rolling every control-plane member before using those routes; see
[metadata-raft-design.md](metadata-raft-design.md#upgrading).

## Example Policies

```text
# policy rules (p)
# p, <role>, <tenant>, <object>, <action>
p, role:tenant-admin, tenant-a, tenant:tenant-a, tenant.manage
p, role:tenant-admin, tenant-a, tenant:tenant-a, rbac.policy.manage
p, role:payments-admin, tenant-a, namespace:tenant-a/payments, ns.manage
p, role:publisher, tenant-a, stream:tenant-a/payments/*, stream.publish

# groupings (g)
# g, <user>, <role>, <tenant>
g, p:alice, role:tenant-admin, tenant-a
g, p:bob, role:payments-admin, tenant-a
```

## Error Semantics

- `401 Unauthorized`: invalid or missing upstream token
- `403 Forbidden`: tenant not allowed, issuer not allowed, or no permissions
- `500 Internal Server Error`: unexpected backend failures

## Future Work

- Policy editor and audit logging for auth decisions.
- More efficient permission compression for large policy sets.
