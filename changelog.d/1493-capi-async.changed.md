- The C ABI (`crates/sdk/felix-capi`) is asynchronous (#1493). `felix_client_connect`,
  `felix_client_publish`, `felix_client_subscribe` and `felix_subscription_next` (which replaces
  `felix_subscription_next_event`) return at once and report their result to a callback, exactly
  once, with the failure's message passed to the callback instead of the per-thread last error.
  Each takes an optional `FelixCancelToken`; a cancelled call, or one pending when its handle is
  freed, completes with the new `FELIX_STATUS_CANCELLED`, and a publish that may already have
  been sent with `FELIX_STATUS_OUTCOME_UNKNOWN`. The blocking forms are gone. A subscription
  allows one pending read at a time.
