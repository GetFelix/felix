- `felix_client_publish` in the C ABI takes `key`, `key_len` and `delivery` after the payload
  (#1493). Pass `NULL, 0, FELIX_DELIVERY_AT_MOST_ONCE` for the previous behaviour.
