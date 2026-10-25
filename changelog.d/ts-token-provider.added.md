- The TypeScript client takes a `tokenProvider` in `Client.connect` and `withIdentity`: a function
  `(signal: AbortSignal) => string | Promise<string>`. The signal aborts at the 30 s fetch timeout and
  once the last handle using the token is released (the client, its `withIdentity` clients, open
  subscriptions); a provider that ignores it is joined, not called again. The client calls it lazily, one call at a time, when
  a stream authenticates without a valid token, and keeps a still-valid token if a fetch fails. It
  is the Rust `RefreshingToken` underneath, and `token` is now optional when a provider is set.
  Breaking: `Client.connect(addrs, tenantId, { token, tokenProvider, serverName, caFile, offerAlpn })`
  and `client.withIdentity(tenantId, { token, tokenProvider })` now take an options object in place
  of the positional `token`, `serverName`, `caFile` and `offerAlpn`. An unknown option or an extra
  positional argument throws a `TypeError`.
