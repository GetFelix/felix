- The C ABI, C#, Python and TypeScript clients send the conditional head
  trim, step 8 of #1662. `felix_client_trim_before` fills a
  `FelixTrimOutcome` (a stale tail is an answer with `trimmed` false and the
  tail); `FelixClient.TrimBeforeAsync` returns a `ConditionalTrim`; Python's
  `trim_before` (sync and async) returns a `ConditionalTrim` and TypeScript's
  `trimBefore` resolves to one. A broker without `FEATURE_HEAD_TRIM` is refused
  locally: `FELIX_STATUS_UNSUPPORTED`, `FelixStatus.Unsupported`, Python's
  `TrimError` and TypeScript's `TrimError`. A trim that timed out is
  outcome unknown in every binding. `client-fixture` gains a `trimmer_token`
  and a `trim_stream`, finalizes `head_trim`, and four `head_trim.*`
  conformance scenarios cover the bindings. The bindings' cursor errors still
  carry only the floor, not the trim's headers.
