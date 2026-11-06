# felix-mqtt

The MQTT listener of a Felix broker: MQTT 3.1.1 and 5.0 over TCP, with TLS
terminated by the broker service. Today it accepts a connection, checks the
token in `CONNECT`, answers pings, and enforces keepalive and the connection
limits. It refuses every `PUBLISH` and `SUBSCRIBE`; mounting streams at topics
and the publish and subscribe paths are later slices of
[docs/mqtt-design.md](../../../docs/mqtt-design.md).

The `ws` module serves the same protocol to browsers: it reads the WebSocket
upgrade, checks the origin and subprotocol, and presents the connection as a byte
stream, so the connection loop and its limits are the same on both transports.

The broker service owns the sockets, the accept loop and the runtime, and hands
each accepted connection to `MqttService`. Packets are encoded and decoded by
[`felix-mqtt-wire`](../../protocol/felix-mqtt-wire). What the listener serves
and how to turn it on is in
[docs/mqtt-compatibility.md](../../../docs/mqtt-compatibility.md).
