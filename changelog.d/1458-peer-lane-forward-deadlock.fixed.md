- Brokers forwarding durable `Quorum` publishes to each other no longer deadlock their
  replication (#1458). Replication shared peer streams with forwarded publishes, and a peer
  answered each stream one request at a time, so a forward waiting on its quorum held up the
  replication that quorum needed until every wait timed out. Replication now has its own peer
  streams and in-flight allowance, and a broker answers forwarded publishes and cache operations
  as they finish rather than in arrival order. Both ends are fixed independently, so a mixed
  cluster improves as soon as either side of a link is upgraded.
- A forwarded publish now waits `FELIX_PUBLISH_QUORUM_TIMEOUT_MS` + 1 s for the owner's answer
  (was `FELIX_INTERNAL_REQUEST_TIMEOUT_MS`, 5 s), so a slow commit returns the owner's result
  instead of an indeterminate one. The publish ack wait floor rises to match: the larger of
  `FELIX_ACK_WAIT_TIMEOUT_MS` and `FELIX_PUBLISH_QUORUM_TIMEOUT_MS` + 1.5 s (6.5 s by default).
- A peer stream whose requests stopped being answered no longer stays wedged for the life of
  the connection (#1458). A requester replaces a stream after two timeouts in a row
  (`felix_broker_peer_lanes_replaced_total`) and offers the new `LANE_RENEWAL` capability, and to
  such a requester a broker drops a stream once a request on it has run past
  `FELIX_INTERNAL_REQUEST_TIMEOUT_MS`. Streams from an older requester are never dropped. After a forwarding storm, peer requests and
  quorum waits on an affected link kept timing out for more than ten minutes with no load.
- A broker serves at most `FELIX_INTERNAL_MAX_SERVED_FORWARDS` (4096) forwarded publishes and
  cache operations at once, and 256 per peer stream; past that a forward is refused as
  `Overload`, which nothing applied and the forwarder may retry (#1458).
