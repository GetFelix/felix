- A revoked token now reaches C and C# callers as an auth failure
  (`FELIX_STATUS_AUTH`) instead of a generic error (#1592), matching the Python
  and TypeScript bindings.
