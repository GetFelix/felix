- C# SDK (`sdks/dotnet`): atomic commits, conditional publishes and idempotent producers.
  `FelixClient.CommitAsync` writes an event and state changes (`CommitOp.Publish`, `Enqueue`, `Put`,
  `Delete`) as one record, `CommitIfAsync` and `PublishIfAsync` write only at an expected offset and
  answer a refusal as `ConditionalWrite` with the tail, and `GetStateAsync` reads a key's value, version
  and as-of offset; each takes `shard:` to name a shard. `CreateProducerAsync` returns a `FelixProducer`
  whose `PublishAsync`, `PublishBatchAsync` and `PublishBatchesAsync` cannot land twice, with
  `AbandonInDoubtAsync`, `GetId`, `CloseAsync(timeout)` and `DisposeAsync` (a graceful close) or
  `Dispose` (frees without waiting). Fixture tests tag the commit, conditional-publish and
  named-shard commit conformance scenarios.
