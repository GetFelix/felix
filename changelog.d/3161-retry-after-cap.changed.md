- `ReconnectPolicy::max_backoff` now caps the wait a broker asks for too (#3161).
  A refusal whose `retry_after_ms` is above `max_backoff`, such as a tenant far
  over its publish quota, is returned to the caller at once with `retry_after_ms`
  in its detail instead of being slept on. This holds for every retrying call:
  plain, at-least-once and idempotent publish, batch and subscribe. A publish
  that runs out of attempts returns the last refusal wrapped in "gave up after N
  attempt(s) over T"; the `BrokerError` is still there to `downcast_ref`.
