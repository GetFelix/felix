- `/token/exchange` now answers `401` for an IdP token whose `iat` is at or
  before its principal's revocation cutoff plus the 60 s IdP clock skew.
  Before, such a token still exchanged after the revocation for the rest of
  its lifetime and started a new 30-day refresh chain. After a revocation,
  including one recorded by an RBAC removal or an issuer change, a client
  that re-exchanges needs an IdP token issued more than 60 s after it; a
  client that refreshes is not affected. `refresh-tokens/revoke` now records
  the cutoff before it ends the principal's refresh chains, and an exchange
  that stored a chain while a revocation landed ends it and answers `401`
  (#2839).
