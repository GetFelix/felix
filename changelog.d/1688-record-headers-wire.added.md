- The wire format can carry a key and headers with each record (#1688). A new
  frame flag, `FLAG_EXTENDED`, opens a `u32` extension word, and its first bit
  puts a length-prefixed headers block before every record of a publish batch or
  event batch. `Auth` and `AuthOk` gain `client_flags_ext` and `server_flags_ext`
  to negotiate it, and `felix-wire` gains `RecordHeaders` to build and check the
  block. No broker or client sends it yet, so every frame on the wire is
  unchanged. See `docs/protocol.md`.
