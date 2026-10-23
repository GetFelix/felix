- A publish that finds the broker's publish queue full no longer stops its
  control stream from reading (#1416). Under `FELIX_PUB_INGRESS_WAIT=1`, and for
  acked publishes under `ack_on_commit`, the publish waits in line off the read
  loop, so publishes behind it to shards with room still go through. A stream
  stops reading only once it has 1024 publishes waiting or its byte budget is
  spent. Order per shard, the queue's first-come admission and the wait timeout
  are unchanged.
