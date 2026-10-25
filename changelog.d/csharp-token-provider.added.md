- C# SDK (`sdks/dotnet`): `FelixClientOptions.TokenProvider`, a `Func<CancellationToken, ValueTask<string>>`
  over the C ABI's token fetch callback, so a client outlives its first token. `Token` becomes optional
  when a provider is set. The library keeps the timing (lazy, refresh at two thirds of a token's life, one
  retry after a refusal, one call at a time, a 30 s wait); the provider's `CancellationToken` fires after
  30 s and on dispose, and its exception message reaches the caller's `FelixException` (`Auth`). The SDK
  now binds every export of `felix.h`.
