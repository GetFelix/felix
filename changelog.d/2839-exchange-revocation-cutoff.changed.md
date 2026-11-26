- After `POST /v1/tenants/{tenant_id}/refresh-tokens/revoke`, `/token/exchange`
  answers `401` for an IdP token of that principal whose `iat` is at or before
  the revocation plus the 60 s IdP clock skew. Before, such a token still
  exchanged for the rest of its lifetime and started a new 30-day refresh
  chain. To exchange again the client needs an IdP token issued more than
  60 s after the revocation. A revocation from an RBAC removal or an issuer
  change does not refuse IdP tokens: exchange still waits out the second and
  mints with the current rules. Under Raft this needs every member at
  metadata version 53; below it the revoke works as before. The revoke now
  records the cutoff before it ends the principal's refresh chains, and an
  exchange that stored a chain while a revocation landed ends it and answers
  `401` (#2839).
