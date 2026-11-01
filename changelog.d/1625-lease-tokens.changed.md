- `felix_authz::PermissionMatcher` skips any permission whose object holds a
  `{` or `}`, and `FelixClaims` has an optional `lease` claim. The control
  plane checks such objects as lease templates when a policy is written, and
  refuses malformed ones.
- A policy object that holds `{` or `}` is now a lease template. An existing
  grant on a literal cache key containing braces, such as
  `cache:t1/prod/c/{leases}`, stops granting that key and instead lets the
  holder of key `K` of cache `t1/prod/leases` get a lease token for `c/K`.
