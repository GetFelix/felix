# felix-authz

Tokens and permissions for [Felix](https://github.com/GetFelix/felix): tenant-scoped
EdDSA tokens, the JWKS a broker verifies them against, and matching an
`action:resource` permission, wildcards included.

The broker uses it to check what a client may do. A client never needs it: it
holds a token and the broker decides.

Not published; it is built into the services. AGPL-3.0-only. See
[LICENSING.md](https://github.com/GetFelix/felix/blob/main/LICENSING.md).
