- The control plane's Raft peer token can be rotated with no outage (#2813).
  `FELIX_RAFT_PEER_TOKEN_NEXT` and `FELIX_RAFT_PEER_TOKEN_PREVIOUS` are accepted on inbound
  peer requests and never sent, so the rotation is three rolling restarts.
  `felix_meta_raft_peer_token_accepted_total{token}` shows when the old token is no longer used.
  The Helm chart reads them from the `next` and `previous` keys of the peer token Secret.
