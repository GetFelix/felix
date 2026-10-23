- Behaviour change: a durable stream now acknowledges a publish after it commits by default
  (#1421). Before, the broker answered when the publish was queued, so a broker crash or a lapsed
  lease could lose a record the client had been told was stored. Committed means written to the
  stream's log under `FELIX_DURABLE_FSYNC_MODE`: with the default `periodic` mode the bytes are in
  the log file (they survive a broker crash, and a machine crash can lose the last 250 ms), and
  `on_commit` flushes to the device first. In-memory streams still answer when the publish is
  queued. Acks now carry their offset on durable streams, and acked publishes cost a write's
  latency. `FELIX_ACK_ON_COMMIT=0` (or `ack_on_commit: false`) restores the old behaviour for every
  stream; `FELIX_ACK_ON_COMMIT=1` makes in-memory streams wait too. The wire is unchanged, so a
  client that predates commit-ack negotiation works as before.
- `FELIX_ACK_ON_COMMIT` now reads `1/true/yes/on` and `0/false/no/off` (trimmed, any case). Any other
  value keeps the default and logs a warning instead of turning commit acks off. A stream with more
  than 1024 answers owed now refuses an acked publish before queueing it, so a retry cannot write
  it twice.
