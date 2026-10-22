- The client's publish writer sends every publish already queued on a stream as one write, and
  reads acks while it keeps writing instead of pausing for them whenever its queue runs dry. The
  broker received about half as many bytes per datagram once keys spread over shards, which
  halved its throughput on small acked publishes (#1406).
