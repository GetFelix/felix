---
title: "Auth and TLS Across Clients"
description: "How to give each Felix client a token, a token provider, a CA file and a server name, and which clients can present a client certificate."
---

Every Felix connection is encrypted with TLS (QUIC has no plaintext mode) and every stream authenticates with a token. This page shows where to set each of those in each client. The language pages explain the options in depth.

## What to set

| You want to | Rust | Python | TypeScript | C# | C |
| --- | --- | --- | --- | --- | --- |
| Give a fixed token | `ClientConfig.auth_token` | `token=` | `token` option | `Token` | `token` argument |
| Give a tenant | `ClientConfig.auth_tenant_id` | `tenant_id=` | second argument to `Client.connect` | `TenantId` | `tenant_id` argument |
| Get a fresh token when one is needed | `ClientConfig.token_provider`, usually a `RefreshingToken` | `token_provider=` (a function or an `async def`) | `tokenProvider` option | `TokenProvider` | `token_fetch` in `FelixConnectOptions` |
| Trust a private CA | `root_store_from_pem_file`, passed to `quic_client_config` | `ca_file=` | `caFile` option | `CaFile` | `ca_file` argument |
| Name the certificate you expect | `server_name` argument to `Client::connect` | `server_name=` | `serverName` option | `ServerName` | `server_name` argument |
| Present a client certificate (mutual TLS) | `ClientIdentity` and `quic_client_config_with_identity` | `client_cert_file=` and `client_key_file=` | `clientCertFile` and `clientKeyFile` options | `ClientCertFile` and `ClientKeyFile` | `client_cert_file` and `client_key_file` in `FelixConnectOptions` |
| Act as another tenant or principal on the same connections | `Client::with_identity` | `with_identity` | `withIdentity` | n/a | n/a |

A client certificate and its key go in as two PEM files, and you give both or neither. [Presenting a client certificate](#presenting-a-client-certificate) says when you need one.

`server_name` defaults to `localhost` in every client except Rust, where you always pass it. Set it to the name in the broker's certificate. With no CA file, a client trusts the operating system's roots.

## Tokens that expire

A token lasts 15 minutes by default, and the broker checks it each time a stream authenticates, including the streams a reconnect opens. A process that outlives one token passes a provider instead of a fixed token. The client calls it when it needs a token:

- when it holds none,
- when the one it holds is past two thirds of its life, or
- when a broker has just refused it.

An idle client makes no calls, and the client never runs two at once, so it is safe to rotate a refresh token inside the provider. A provider that fails or takes more than 30 seconds fails the call, unless the token already held is still valid, in which case the client keeps using it.

The full example for each language, including the control plane's refresh exchange:

- [Rust](/clients/rust/#getting-and-refreshing-a-token)
- [Python](/clients/python/#getting-and-refreshing-a-token)
- [TypeScript](/clients/typescript/#getting-and-refreshing-a-token)
- [C#](/clients/csharp/#getting-and-refreshing-a-token)
- [C](/clients/c/#getting-and-refreshing-a-token)
- [Authentication that lasts](/clients/resilient-clients/#authentication-that-lasts), with the same recipe side by side in every language.

[Getting a token](/clients/overview/#getting-a-token) says where a token comes from in the first place.

## Trusting the broker

By default a client trusts the operating system's root certificates. A broker with a self-signed or private-CA certificate needs the CA file, and the `server_name` must match a name in that certificate. A mismatch fails the connect.

The language pages show the TLS options in context:
[Rust](/clients/rust/#tls-and-alpn), [Python](/clients/python/#connecting), [TypeScript](/clients/typescript/#connecting).

## Presenting a client certificate

A broker started with `FELIX_TLS_CLIENT_CA` requires every client to present a certificate issued by that CA, on top of the token. Without one the handshake is refused and the connect fails. Pass the certificate and its key as PEM files, next to the CA file. The certificate file holds the leaf first and then any intermediates, and the key file holds one PKCS#8, PKCS#1 or SEC1 key.

The option is the same in every client, apart from its spelling. This is the Python form:

```python
import felix

with felix.Client(
    "felix-0:5000",
    tenant_id="t1",
    token=tok,
    ca_file="ca.pem",
    client_cert_file="client.pem",
    client_key_file="client.key",
) as client:
    client.publish("t1", "app", "events", b"hello")
```

A missing file, a file that is not PEM, a key that does not match the certificate, or only one of the two is reported as an invalid argument before anything is sent, and the message names the file. The language pages show the form for their client: [Rust](/clients/rust/#tls-and-alpn), [Python](/clients/python/#connecting), [TypeScript](/clients/typescript/#connecting), [C#](/clients/csharp/) and [C](/clients/c/).

## What the broker checks

The broker verifies the token's signature and expiry, reads the tenant and permissions from it, and checks them on every operation. A token for one tenant cannot touch another tenant's data. A broker started with `FELIX_TLS_CLIENT_CA` also requires a client certificate. [Security](/features/security/) has the full model, and [Error Codes](/reference/error-codes/) lists what a refusal looks like. A refusal shows up in your code as the `unauthenticated`, `forbidden` or `revoked` code: see [Errors Across Clients](/clients/errors/).
