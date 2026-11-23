- The control plane keeps an audit log of who changed what (#2806). Every
  write on its API and bootstrap listener, and every refused request, is
  recorded with the verified subject, credential kind, source address, route
  and status, never a token or body. Records are committed through the
  metadata Raft log (metadata level 49), so one member cannot rewrite them;
  every member keeps the newest 10,000 and logs each at target `felix_audit`.
  Read them with `GET /v1/audit` (needs `audit.view:cluster:*`) or
  `felixctl audit ls [--follow] [--json]`. Records not committed are counted in
  `felix_audit_records_dropped_total`.
