# Felix Wire Test Vectors

These vectors are the source of truth for cross-language encoding/decoding.
Every client implementation should validate against them.

Files:
- `publish.json` / `publish.hex`
- `subscribe.json` / `subscribe.hex`
- `event.json` / `event.hex`
- `cache_put.json` / `cache_put.hex`
- `cache_get.json` / `cache_get.hex`
- `cache_value.json` / `cache_value.hex`
- `ok.json` / `ok.hex`
- `error.json` / `error.hex`
- `binary_publish_acked.json` / `.hex` (binary, flags `0x0009`)
- `binary_publish_keyed.json` / `.hex` (binary, flags `0x0041`)
- `binary_publish_acked_keyed.json` / `.hex` (binary, flags `0x0049`)
- `binary_publish_headers.json` / `.hex` (binary, flags `0x8001`, record headers extension)
- `binary_publish_ack_ok.json` / `.hex` (binary, flags `0x0010`)
- `binary_publish_ack_error.json` / `.hex` (binary, flags `0x0010`)
- `auth_with_capabilities.json` / `.hex` (capability offer on the auth handshake)
- `auth_ok.json` / `.hex` (the broker's capability advertisement)
- `auth_extended_features.json` (extended feature bits offered on the auth handshake)
- `auth_ok_extended_features.json` (extended feature bits in the broker's answer)

Each JSON file describes:
- message type and fields
- raw payload (exact bytes)
- expected framed bytes (hex)

Vectors carry an optional `kind` field selecting how they are validated:
- absent or `json_message` — a JSON `Message` payload with `flags` = 0
- `binary_publish_batch` — the binary publish batch encoding, keyed or not
- `binary_acked_publish_batch` — the binary acked publish encoding
- `binary_publish_batch_headers` — a publish batch with `FLAG_EXTENDED` and record headers blocks
- `binary_publish_ack` — the binary publish acknowledgement

Binary vectors also carry `flags`, plus the decoded field values so a client can
check its decoder as well as its encoder. A publish vector carrying `key_utf8`
sets `FLAG_BINARY_PUBLISH_KEYED` (`0x0040`) and prefixes its body with a `u16`
key length and the key bytes, after the acked prefix when there is one.

Note: payloads are UTF-8 JSON, and binary values are base64-encoded.

Hex files contain lowercase hex without spaces or newlines.
