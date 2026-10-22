- Per-tenant publish counters and the subscriber-queue metrics are resolved once and reused
  instead of looked up in the metrics registry on every publish (#1406). On a broker publishing
  small batches this removed about 3% of CPU spent hashing and registering metric keys.
  `felix_broker_ingress_queue_depth` is now refreshed when the depth has moved by 16 or more
  since the last refresh and when it reaches zero, so while jobs are queued it can differ from
  the true depth by up to 15; an emptied queue reads zero.
