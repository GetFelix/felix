- The TypeScript client takes a `tokenProvider` in `Client.connect` and `withIdentity`: a function
  returning a Felix token or a promise of one. The client calls it lazily, one call at a time, when
  a stream authenticates without a valid token, and keeps a still-valid token if a fetch fails. It
  is the Rust `RefreshingToken` underneath, and `token` is now optional when a provider is set.
