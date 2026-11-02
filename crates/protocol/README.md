# Protocol

How peers talk. All three crates are Apache-2.0 so that anyone can build a
client or a server without taking on a copyleft dependency.

- [`felix-wire`](felix-wire) is the frame codec: the frame header, the
  `Message` enum, the binary batch formats, and the broker-to-broker messages.
  It does no I/O. `docs/protocol.md` is the specification it implements.
- [`felix-transport`](felix-transport) is the QUIC layer: endpoints,
  connection and stream lifetime, and the transport tuning shared by the client
  and the broker. TLS configuration comes from the caller. It knows nothing
  about Felix messages.

- [`felix-mqtt-wire`](felix-mqtt-wire) is the MQTT 3.1.1 and 5.0 packet codec
  for the broker's MQTT listener. It does no I/O, enforces its size limits
  while decoding, and is not published to crates.io. `docs/mqtt-design.md`
  explains where it fits.

None depends on any other crate in this repository.
