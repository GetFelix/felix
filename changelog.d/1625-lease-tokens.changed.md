- `felix_authz::PermissionMatcher` skips any permission whose object holds a
  `{` or `}`, and `FelixClaims` has an optional `lease` claim. The control
  plane checks such objects as lease templates when a policy is written, and
  refuses malformed ones.
