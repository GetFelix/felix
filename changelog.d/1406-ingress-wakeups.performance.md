- The publish scheduler wakes an ingress executor only when a lane becomes ready and an
  executor is waiting, a connection's read pass wakes at most one executor per queue, and per-tenant publish counters are resolved once per identity
  instead of looked up on every publish (#1406). This cuts cross-thread wake-ups and
  metrics-registry lookups when load is spread thinly over many shards.
  `felix_broker_ingress_queue_depth` is now refreshed when the depth has moved by 16 or
  more since the last refresh and when it reaches zero, so while jobs are queued it can
  differ from the true depth by up to 15; an emptied queue reads zero.
- `FELIX_INGRESS_SPIN_US` (default `0`, off) makes an idle publish executor watch the queue
  for that many microseconds before it parks, so a publish that arrives during the window needs
  no wake-up. It trades CPU for latency (#1406).
