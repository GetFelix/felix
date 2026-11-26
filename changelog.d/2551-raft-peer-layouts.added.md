- The peer handshake now states which Raft message layouts a broker reads (`RAFT_LAYOUTS`)
  (#2551). A sender whose layout the peer does not read refuses before sending any Raft
  message, with a reason naming both, a warning log and
  `felix_broker_raft_peer_refusals_total{reason="layout"}`, instead of reading as a plain
  unreachable peer. A peer that states no layouts is read as layout 6.
