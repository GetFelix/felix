# felix-mqtt-wire

The MQTT 3.1.1 and 5.0 packet codec for the broker's MQTT listener. It turns
bytes into `Packet` values and back. It does no I/O and keeps no session state,
and it refuses a hostile packet while decoding it, with every size bounded by
`Limits`. It is internal to the Felix repository and is not published to
crates.io. `docs/mqtt-design.md` in the repository says why Felix owns this
codec and where it fits.

## Using it

Decode from a buffer you read the socket into, and encode into one you write
from. Before the client's `CONNECT` the connection has no version, so pass
`None`; after it, pass the version the `CONNECT` named.

```rust
use bytes::BytesMut;
use felix_mqtt_wire::{
    ConnAck, Limits, Packet, Properties, QoS, ReasonCode, Version,
};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let limits = Limits::default();
let mut inbound = BytesMut::new();
let mut outbound = BytesMut::new();

// A 5.0 CONNECT for client "dev-1", then a QoS 0 PUBLISH of "hi" to "a/b".
inbound.extend_from_slice(b"\x10\x12\x00\x04MQTT\x05\x02\x00\x1e\x00\x00\x05dev-1");
inbound.extend_from_slice(b"\x30\x08\x00\x03a/b\x00hi");

let mut version = None;
while let Some(packet) = Packet::decode(&mut inbound, version, &limits)? {
    match packet {
        Packet::Connect(connect) => {
            version = Some(connect.version);
            let reply = Packet::ConnAck(ConnAck {
                session_present: false,
                reason: ReasonCode::Success,
                properties: Properties::default(),
            });
            reply.encode(connect.version, &mut outbound)?;
        }
        Packet::Publish(publish) => {
            assert_eq!(publish.topic, "a/b");
            assert_eq!(publish.qos, QoS::AtMostOnce);
            assert_eq!(&publish.payload[..], b"hi");
        }
        _ => {}
    }
}
assert_eq!(version, Some(Version::V5));
assert_eq!(&outbound[..], b"\x20\x03\x00\x00\x00");
# Ok(())
# }
```

`Packet::decode` returns `Ok(None)` when the buffer does not hold a whole packet
yet and takes nothing from it, so call it again after the next read. An `Err`
means close the connection: the stream cannot be resynchronised.
`DecodeError::reason_code` gives the 5.0 `DISCONNECT` reason that fits.

Strings and payloads are slices of the buffer the packet arrived in. Nothing is
copied, and a slice keeps that whole buffer allocation alive while it is held,
so copy any string or payload you keep past the packet (a session's client
identifier, a filter, a retained payload).

The default `Limits` allow a 1 MiB packet. Before `CONNECT` is accepted, decode
with a smaller `max_packet_size`, since a device can announce a large packet
with five bytes.

To send a `PUBLISH` without copying a payload that already lives elsewhere,
write `Publish::encode_head` and then the payload. `Packet::encoded_len` sizes a
packet, for checking a client's `Maximum Packet Size` before sending.

## What it checks

Everything the specification makes malformed or a protocol error from the
packet alone:

- The fixed header: reserved packet types, the fixed flag bits, a remaining
  length that is minimal and at most four bytes.
- Field syntax: strings are well-formed UTF-8 with no U+0000, lengths fit, and
  nothing is left over after the last field.
- Reserved bits and values in `CONNECT` flags, subscription options and
  `CONNACK`, QoS 3, packet identifier 0, and DUP on a QoS 0 `PUBLISH`.
- Reason codes, against the table for each packet. 3.1.1's return codes map onto
  the 5.0 `ReasonCode` with the same meaning.
- 5.0 properties: each may appear only in the packets the specification lists,
  once (except user properties and the subscription identifiers of a
  `PUBLISH`), with a valid value.
- Topic names and filters: wildcard placement, `$share/` syntax under 5.0, and
  no control characters or Unicode non-characters, which the specification
  says a string should not hold.

`Limits` bounds what a device can make the decoder hold: whole packet size,
topic bytes and levels, filters per `SUBSCRIBE`, repeated properties in a
property block, and client identifier length. The packet size is checked from
the fixed header, before the body is waited for. Lists grow only as bytes that
passed their checks arrive, never from a count the peer sent, and stop at the
limit.

It leaves to the caller what depends on the connection: which packet may arrive
in which state or direction, whether an empty client identifier is acceptable
with a clean start of `false`, topic alias and subscription identifier use,
whether a payload marked as UTF-8 is, keepalive, and packet identifier reuse.

`Packet::encode` refuses what the decoder refuses, so a packet it writes is one
a peer accepts. Under 5.0 it omits a `PUBACK`, `PUBREC`, `PUBREL`, `PUBCOMP` or
`DISCONNECT` reason code and properties when they are `Success` and empty, as
the specification allows; the decoder accepts every form.

## Tests

- `tests/golden.rs` decodes and re-encodes packets that rumqttc 0.25.1 wrote
  (`tests/vectors/rumqttc.txt`, made by `tests/vectors/rumqttc-gen`), plus
  packets built from the specification.
- `tests/malformed.rs` has one case for each way a packet can be refused, with
  the specification statement it comes from.
- `tests/generated.rs` round-trips random packets and feeds the decoder damaged
  and random bytes.
- `fuzz/` is the libFuzzer target `mqtt_packet`; `task fuzz` runs it, and CI
  and the nightly workflow run it with the other decoders.
