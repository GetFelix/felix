- The peer handshake now states which Raft message layouts a broker reads (`RAFT_LAYOUTS`),
  and a sender writes one both sides read (#2551). When two builds share none, Raft
  messages to that peer are refused before any is sent, with a reason naming both ranges,
  a warning log and `felix_broker_raft_peer_refusals_total{reason="layout"}`, instead of
  reading as a plain unreachable peer. A peer that states no layouts is read as layout 6.
