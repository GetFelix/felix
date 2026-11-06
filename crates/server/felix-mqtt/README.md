# felix-mqtt

The MQTT listener of a Felix broker: MQTT 3.1.1 and 5.0 over TCP, with TLS
terminated by the broker service. Today it accepts a connection, checks the
token in `CONNECT`, answers pings, enforces keepalive and the connection
limits, and accepts `PUBLISH` at QoS 0 and 1 to streams mounted at topic
prefixes. It refuses `SUBSCRIBE`; the subscribe path is a later slice of
[docs/mqtt-design.md](../../../docs/mqtt-design.md).

The `ws` module serves the same protocol to browsers: it reads the WebSocket
upgrade, checks the origin and subprotocol, and presents the connection as a byte
stream, so the connection loop and its limits are the same on both transports.

The broker service owns the sockets, the accept loop and the runtime, and hands
each accepted connection to `MqttService`. Packets are encoded and decoded by
[`felix-mqtt-wire`](../../protocol/felix-mqtt-wire). What the listener serves
and how to turn it on is in
[docs/mqtt-compatibility.md](../../../docs/mqtt-compatibility.md).
