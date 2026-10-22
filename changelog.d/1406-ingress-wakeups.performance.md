- The publish scheduler wakes an ingress executor only when a lane becomes ready and an
  executor is waiting, and per-tenant publish counters are resolved once per identity
  instead of looked up on every publish (#1406). This cuts cross-thread wake-ups and
  metrics-registry lookups when load is spread thinly over many shards.
  `felix_broker_ingress_queue_depth` is now set when the depth moves by 16 or more and
  when it reaches zero, so it can read up to 15 below or above the true depth while
  jobs are queued.
