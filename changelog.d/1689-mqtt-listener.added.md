- A broker can accept MQTT 3.1.1 and 5.0 connections: set `FELIX_MQTT_LISTEN`
  (#1689). The listener runs on its own tokio runtime, caps connections per
  broker and per source address, paces new ones, and authenticates `CONNECT`
  with the tenant id and a Felix token. It answers pings and enforces keepalive,
  and ends a connection when its token expires or is revoked. Every `PUBLISH`
  and `SUBSCRIBE` is refused until the publish and subscribe paths land. With
  the variable unset the broker is unchanged. See `docs/mqtt-compatibility.md`.
