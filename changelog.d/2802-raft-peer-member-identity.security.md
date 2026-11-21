- Control-plane Raft peers now prove which member they are (#2802). **Breaking.**
  Under peer mTLS (`FELIX_RAFT_TLS_CERT`, `_KEY`, `_CA`) each member's leaf
  certificate must carry exactly one URI SAN,
  `felix:principal:raft-member:<cluster_id>:<member_id>`. A vote, append,
  snapshot, forming record or standing `from` that speaks for another member is
  refused with `403`, and a member exits at startup under a certificate naming
  another member or a revoked one. A member also refuses to connect to a peer
  serving a revoked certificate. `FELIX_RAFT_TLS_REVOKED` lists SHA-256
  fingerprints of revoked certificates; after wiping a member, issue a new
  certificate and revoke the old one on every member. With TLS set,
  `FELIX_RAFT_PEER_TOKEN` is optional; the token alone still works, is
  insecure, and logs a startup warning. The peer `propose` route refuses
  `ImportState` (`403`); the operator import is now
  `POST /internal/bootstrap/import-state` on the bootstrap listener (leader
  only, a follower answers `421 not_leader`), and the `waive-lost` peer route
  moved to `POST /internal/bootstrap/raft/waive-lost`. Both take the operator
  token in `X-Felix-Operator-Token`, not the bootstrap token every member
  holds; members get only its digest, `FELIX_OPERATOR_TOKEN_SHA256`, and the
  routes answer `403` without it (Helm: `controlplane.bootstrap.operatorTokenSha256`).
  To upgrade, first reissue every member's certificate with its URI SAN (older
  builds check only the CA and accept it), then roll the new build one member
  at a time, keeping `FELIX_RAFT_PEER_TOKEN` until all members run it. The Helm
  chart now reads one Secret per member, `<secretPrefix>-<member id>`, in place
  of `controlplane.storage.raft.tls.existingSecret`.
