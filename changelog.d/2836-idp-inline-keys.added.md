- An identity provider issuer can carry its public keys inline (`jwks`, several keys for a rotation) instead of a
  `jwks_url` or discovery, and the control plane then never fetches for it. Per issuer, `single_use_jti` refuses a
  replayed `jti` until its `exp` (held in memory per control plane replica, so single use holds per replica) and
  `max_token_lifetime_seconds` refuses a token whose `exp - iat` is larger. The new fields need metadata level 42
  (#2836).
