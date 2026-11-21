- Control-plane Raft peers now prove which member they are (#2802). **Breaking.**
  Under peer mTLS (`FELIX_RAFT_TLS_CERT`, `_KEY`, `_CA`) each member's leaf
  certificate must carry exactly one URI SAN,
  `felix:principal:raft-member:<cluster_id>:<member_id>`. A vote, append,
  snapshot, forming record or standing `from` that speaks for another member is
  refused with `403`, and a member refuses to start under a certificate naming
  another member or a revoked one. `FELIX_RAFT_TLS_REVOKED` lists SHA-256
  fingerprints of revoked certificates; after wiping a member, issue a new
  certificate and revoke the old one on every member. With TLS set,
  `FELIX_RAFT_PEER_TOKEN` is optional; the token alone still works, is
  insecure, and logs a startup warning. The peer `propose` route refuses
  `ImportState` (`403`); the operator import is now
  `POST /internal/bootstrap/import-state` on the bootstrap listener (leader
  only, a follower answers `421 not_leader`), and the `waive-lost` peer route
  moved to `POST /internal/bootstrap/raft/waive-lost` with the bootstrap token.
  To upgrade, first reissue every member's certificate with its URI SAN (older
  builds check only the CA and accept it), then roll the new build one member
  at a time. The Helm chart mounts one certificate for all members, so it
  cannot carry a per-member SAN yet.
