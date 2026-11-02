- MQTT over WebSockets for browser clients (#1700): set `FELIX_MQTT_WS_LISTEN`
  next to `FELIX_MQTT_LISTEN` and the broker serves `ws` or `wss` (following
  `FELIX_MQTT_TLS`) on a second socket. It shares the MQTT listener's runtime,
  limits, connection caps and authentication. The upgrade has a header size
  limit (`FELIX_MQTT_WS_MAX_HEADER_BYTES`), a time limit
  (`FELIX_MQTT_WS_UPGRADE_TIMEOUT_MS`) and an `Origin` allow-list
  (`FELIX_MQTT_WS_ALLOWED_ORIGINS`; a browser is refused until it is set).
  With the variable unset the broker is unchanged. See
  `docs/mqtt-compatibility.md`.
