# Bootstrap security

The bootstrap endpoint (`POST /internal/bootstrap/tenants/{tenant_id}/initialize`)
is the one place Felix hands out admin-equivalent power to a caller that does
not yet have a Felix token. That is its entire reason to exist, and what makes
it the most security-sensitive surface in the control plane. This document is
the threat model and the operating rules. The mechanics of what bootstrap
seeds are in [auth.md](../auth.md#bootstrap-mode-day-0).

## What an attacker gets

A successful unauthorized bootstrap on a **new** tenant id creates that tenant
with the attacker's IdP issuer and the attacker's principals as tenant admins:
a full tenant under attacker control, on infrastructure you run. Against an
**existing, already-initialized** tenant the call returns `409` and changes
nothing; initialization is exactly-once (see below). The window that matters is
therefore a tenant that exists but is not yet initialized, and the ability to
mint tenants that should not exist.

Bootstrap tokens never authorize normal admin endpoints, so the blast radius is
bounded to initialization, but initialization is enough.

## Layers, and what each one stops

| Layer | Stops | Configured by |
| --- | --- | --- |
| Disabled by default | everything, when you are not bootstrapping | `FELIX_BOOTSTRAP_ENABLED` (default `false`) |
| Separate listener on a loopback default | off-host callers, unless an operator deliberately exposes it | `FELIX_BOOTSTRAP_BIND_ADDR` (default `127.0.0.1:9095`) |
| Shared token, compared in constant time | anyone without the secret | `FELIX_BOOTSTRAP_TOKEN` |
| mTLS with a required client CA | anyone without a client certificate, *before* any request is read | `FELIX_BOOTSTRAP_TLS_CERT` / `_KEY` / `_CLIENT_CA` |

The layers are independent. The token is required whenever bootstrap is
enabled; mTLS is optional and recommended anywhere the listener is reachable
beyond one machine. With mTLS configured, an unauthenticated client is refused
at the TLS handshake. The token, valid or not, is never seen, and no handler
runs.

```
FELIX_BOOTSTRAP_TLS_CERT=/etc/felix/bootstrap/server.pem
FELIX_BOOTSTRAP_TLS_KEY=/etc/felix/bootstrap/server.key
FELIX_BOOTSTRAP_TLS_CLIENT_CA=/etc/felix/bootstrap/client-ca.pem
```

All three or none: a partial set fails startup rather than coming up
half-secured, and there is deliberately no TLS-without-client-CA mode. A
bootstrap listener that encrypts but does not authenticate would look secured
while stopping nobody. The same variables (or the `bootstrap.tls` YAML block)
must be identical on every control-plane instance, like the token itself.

## Token lifetime and replay

The bootstrap token is a static shared secret, valid for as long as bootstrap
is enabled. It is **not** one-time, and a captured token can be replayed
against any not-yet-initialized tenant until the operator disables bootstrap or
rotates it. Three consequences:

- **Disable bootstrap after use.** `FELIX_BOOTSTRAP_ENABLED=false` is the real
  end of the token's life; treat leaving it enabled as leaving a door open.
- **Initialization itself cannot be replayed.** The whole seed (signing keys,
  issuers, RBAC, and the bootstrapped flag) commits as one atomic, exactly-once
  store operation, serialized on the tenant row. Replaying the call, from the
  same client or through a different control-plane instance, returns `409
  already_initialized` and writes nothing.
- **A failure part-way leaves the tenant retryable.** Nothing marks the tenant
  bootstrapped unless everything else committed with it, so the recovery from a
  mid-bootstrap crash is to run the same call again.

## Rotation without an outage

Two tokens are accepted while a rotation is in flight: the current one and the
one being retired.

1. Deploy every instance with `FELIX_BOOTSTRAP_TOKEN=<new>` and
   `FELIX_BOOTSTRAP_TOKEN_PREVIOUS=<old>`. During the rolling deploy both
   generations of instance accept both tokens, so no caller is refused for
   hitting the wrong generation.
2. Move callers to the new token.
3. Deploy again without `FELIX_BOOTSTRAP_TOKEN_PREVIOUS`.

Setting only the previous token fails startup, because that shape means the rotation
removed the wrong half.

## Multiple instances

Every instance validates against the same configuration, so a bootstrap call
behaves identically through any of them; there is no instance affinity to
arrange. Concurrent initializes of the same tenant through different instances
are the normal load-balanced case, and exactly one wins. The store's
transaction, not a check in the handler, decides which. The losers get `409`,
and the winner's reported signing-key id is guaranteed to be the key the
tenant actually holds.

## Recovery

- **Expired or lost token:** set a new `FELIX_BOOTSTRAP_TOKEN` and redeploy.
  Nothing durable derives from the old token; it is only compared, never
  stored.
- **Rotated credentials mid-deploy refused:** verify both halves are set
  everywhere (`token` = new, `previous_token` = old); an instance with only the
  new token refuses the old one.
- **Tenant stuck uninitialized after an error:** the call is retryable as-is;
  the bootstrapped flag only commits with a complete seed.
- **Tenant initialized with the wrong seed:** bootstrap will not overwrite it
  (`409`). Fix forward with the normal admin endpoints using a token from the
  seeded IdP, or remove the tenant and bootstrap again.

Never log or echo the token; the control plane does not, and requests carrying
it should not transit anything that logs headers.
