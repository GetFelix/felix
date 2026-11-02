- Lease tokens (#1625). A `cache_put_if` with `lease_token: true` that applies
  returns a short-lived broker token granting what the caller's lease templates
  (RBAC objects such as `stream:t1/prod/wl.{leases}.out`) name for the key, so
  nodes sharing an identity can write a workload's output only while they hold
  its lease. The key's owner mints it with a key every broker shares
  (`FELIX_LEASE_TOKEN_KEY_FILE`), with no control-plane call on the claim, and
  a lease token's open streams end at its `exp`. Negotiated as
  `FEATURE_LEASE_TOKEN`; forwarded as the new peer kinds 45 and 46. Rust client
  only: `cache_put_if_lease` on `Connection` and `Client`.
