//! Generated packets and mutated bytes.
//!
//! Random packets are built from public types, encoded, and decoded again.
//! The encoder refuses what it should not write, so a packet it accepts has to
//! decode to itself; a packet it refuses is skipped. Their encodings are then
//! cut, flipped and padded, and the decoder must never panic, and whatever it
//! does accept must survive another round trip.

use bytes::{Bytes, BytesMut};
use felix_mqtt_wire::{
    Ack, Auth, ConnAck, Connect, Disconnect, Limits, Packet, Properties, Publish, QoS, ReasonCode,
    RetainHandling, SubAck, Subscribe, SubscribeFilter, UnsubAck, Unsubscribe, Utf8, Version, Will,
};

/// xorshift64*: deterministic, so a failure names a seed that reproduces it.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn chance(&mut self, one_in: usize) -> bool {
        self.below(one_in) == 0
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }

    fn bytes(&mut self, max: usize) -> Bytes {
        let n = self.below(max + 1);
        Bytes::from((0..n).map(|_| self.next() as u8).collect::<Vec<_>>())
    }

    fn some<T>(&mut self, make: impl FnOnce(&mut Self) -> T) -> Option<T> {
        if self.chance(2) {
            Some(make(self))
        } else {
            None
        }
    }

    fn text(&mut self, max: usize) -> Utf8 {
        const ALPHABET: &[&str] = &["a", "b", "z", "0", "9", "-", "_", " ", "é", "€", "😀", "日"];
        let n = self.below(max + 1);
        let s: String = (0..n).map(|_| *self.pick(ALPHABET)).collect();
        Utf8::try_from(s).unwrap()
    }

    fn topic(&mut self) -> Utf8 {
        let levels = 1 + self.below(5);
        let parts: Vec<String> = (0..levels)
            .map(|_| {
                let s = self.text(4);
                if s.is_empty() {
                    "x".into()
                } else {
                    s.as_str().replace('/', "_")
                }
            })
            .collect();
        Utf8::try_from(parts.join("/")).unwrap()
    }

    fn filter(&mut self, v5: bool) -> Utf8 {
        let levels = 1 + self.below(4);
        let mut parts: Vec<String> = (0..levels)
            .map(|_| match self.below(5) {
                0 => "+".to_string(),
                _ => {
                    let s = self.text(3);
                    if s.is_empty() {
                        "q".into()
                    } else {
                        s.as_str().to_string()
                    }
                }
            })
            .collect();
        if self.chance(3) {
            parts.push("#".into());
        }
        let mut filter = parts.join("/");
        if v5 && self.chance(5) {
            filter = format!("$share/g{}/{filter}", self.below(3));
        }
        Utf8::try_from(filter).unwrap()
    }

    fn qos(&mut self) -> QoS {
        *self.pick(&[QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce])
    }

    fn reason(&mut self) -> ReasonCode {
        loop {
            if let Some(r) = ReasonCode::from_code(self.next() as u8) {
                return r;
            }
        }
    }

    fn user_properties(&mut self) -> Vec<(Utf8, Utf8)> {
        (0..self.below(3))
            .map(|_| (self.text(5), self.text(8)))
            .collect()
    }

    fn packet_id(&mut self) -> u16 {
        1 + self.below(65535) as u16
    }
}

/// A property block with a random selection of properties. Most are rejected
/// somewhere (they do not belong in every packet), which `encode` decides.
fn properties(rng: &mut Rng) -> Properties {
    let mut p = Properties::default();
    if rng.chance(4) {
        p.payload_format_indicator = Some(rng.chance(2));
    }
    if rng.chance(4) {
        p.message_expiry_interval = Some(rng.next() as u32);
    }
    if rng.chance(4) {
        p.content_type = Some(rng.text(10));
    }
    if rng.chance(4) {
        p.response_topic = Some(rng.topic());
    }
    if rng.chance(4) {
        p.correlation_data = Some(rng.bytes(10));
    }
    if rng.chance(4) {
        p.subscription_identifiers = (0..1 + rng.below(3))
            .map(|_| 1 + rng.next() as u32 % 1000)
            .collect();
    }
    if rng.chance(4) {
        p.session_expiry_interval = Some(rng.next() as u32);
    }
    if rng.chance(6) {
        p.assigned_client_identifier = Some(rng.text(8));
    }
    if rng.chance(6) {
        p.server_keep_alive = Some(rng.next() as u16);
    }
    if rng.chance(6) {
        p.authentication_method = Some(rng.text(8));
        p.authentication_data = rng.some(|r| r.bytes(8));
    }
    if rng.chance(6) {
        p.request_problem_information = Some(rng.chance(2));
        p.request_response_information = Some(rng.chance(2));
    }
    if rng.chance(6) {
        p.will_delay_interval = Some(rng.next() as u32);
    }
    if rng.chance(6) {
        p.response_information = Some(rng.text(8));
        p.server_reference = Some(rng.text(8));
    }
    if rng.chance(3) {
        p.reason_string = Some(rng.text(12));
    }
    if rng.chance(6) {
        p.receive_maximum = Some(1 + rng.next() as u16 % 100);
        p.topic_alias_maximum = Some(rng.next() as u16);
        p.maximum_packet_size = Some(1 + rng.next() as u32 % 100_000);
    }
    if rng.chance(6) {
        p.topic_alias = Some(1 + rng.next() as u16 % 100);
    }
    if rng.chance(6) {
        p.maximum_qos = Some(*rng.pick(&[QoS::AtMostOnce, QoS::AtLeastOnce]));
        p.retain_available = Some(rng.chance(2));
        p.wildcard_subscription_available = Some(rng.chance(2));
        p.subscription_identifiers_available = Some(rng.chance(2));
        p.shared_subscription_available = Some(rng.chance(2));
    }
    p.user_properties = rng.user_properties();
    p
}

/// Properties, but only some of the time, so empty blocks are common.
fn maybe_properties(rng: &mut Rng) -> Properties {
    if rng.chance(2) {
        properties(rng)
    } else {
        Properties::default()
    }
}

fn reasons(rng: &mut Rng) -> Vec<ReasonCode> {
    (0..1 + rng.below(4)).map(|_| rng.reason()).collect()
}

fn random_packet(rng: &mut Rng, v5: bool) -> (Packet, Version) {
    let version = if v5 { Version::V5 } else { Version::V311 };
    let props = |rng: &mut Rng| {
        if v5 {
            maybe_properties(rng)
        } else {
            Properties::default()
        }
    };
    let packet = match rng.below(15) {
        0 => Packet::Connect(Connect {
            version,
            clean_start: rng.chance(2),
            keep_alive: rng.next() as u16,
            client_id: rng.text(30),
            will: rng.some(|r| Will {
                topic: r.topic(),
                payload: r.bytes(40),
                qos: r.qos(),
                retain: r.chance(2),
                properties: if v5 {
                    maybe_properties(r)
                } else {
                    Properties::default()
                },
            }),
            username: rng.some(|r| r.text(10)),
            password: rng.some(|r| r.bytes(10)),
            properties: props(rng),
        }),
        1 => Packet::ConnAck(ConnAck {
            session_present: rng.chance(2),
            reason: rng.reason(),
            properties: props(rng),
        }),
        2 => {
            let qos = rng.qos();
            Packet::Publish(Publish {
                dup: rng.chance(3),
                qos,
                retain: rng.chance(2),
                topic: if v5 && rng.chance(8) {
                    Utf8::default()
                } else {
                    rng.topic()
                },
                packet_id: (qos != QoS::AtMostOnce).then(|| rng.packet_id()),
                properties: props(rng),
                payload: rng.bytes(300),
            })
        }
        3..=6 => {
            let ack = Ack {
                packet_id: rng.packet_id(),
                reason: if v5 {
                    rng.reason()
                } else {
                    ReasonCode::Success
                },
                properties: props(rng),
            };
            match rng.below(4) {
                0 => Packet::PubAck(ack),
                1 => Packet::PubRec(ack),
                2 => Packet::PubRel(ack),
                _ => Packet::PubComp(ack),
            }
        }
        7 => Packet::Subscribe(Subscribe {
            packet_id: rng.packet_id(),
            filters: (0..1 + rng.below(4))
                .map(|_| {
                    let mut f = SubscribeFilter::new(rng.filter(v5), rng.qos());
                    if v5 {
                        f.no_local = rng.chance(2);
                        f.retain_as_published = rng.chance(2);
                        f.retain_handling = *rng.pick(&[
                            RetainHandling::SendAtSubscribe,
                            RetainHandling::SendIfNew,
                            RetainHandling::DoNotSend,
                        ]);
                    }
                    f
                })
                .collect(),
            properties: if v5 && rng.chance(2) {
                Properties {
                    subscription_identifiers: vec![1 + rng.next() as u32 % 5000],
                    user_properties: rng.user_properties(),
                    ..Properties::default()
                }
            } else {
                Properties::default()
            },
        }),
        8 => Packet::SubAck(SubAck {
            packet_id: rng.packet_id(),
            reasons: reasons(rng),
            properties: props(rng),
        }),
        9 => Packet::Unsubscribe(Unsubscribe {
            packet_id: rng.packet_id(),
            filters: (0..1 + rng.below(3)).map(|_| rng.filter(v5)).collect(),
            properties: props(rng),
        }),
        10 => Packet::UnsubAck(UnsubAck {
            packet_id: rng.packet_id(),
            reasons: if v5 { reasons(rng) } else { Vec::new() },
            properties: props(rng),
        }),
        11 => Packet::PingReq,
        12 => Packet::PingResp,
        13 => Packet::Disconnect(Disconnect {
            reason: if v5 {
                rng.reason()
            } else {
                ReasonCode::Success
            },
            properties: props(rng),
        }),
        _ => Packet::Auth(Auth {
            reason: rng.reason(),
            properties: properties(rng),
        }),
    };
    (packet, version)
}

fn decode(
    bytes: &[u8],
    version: Option<Version>,
) -> Result<Option<Packet>, felix_mqtt_wire::DecodeError> {
    Packet::decode(&mut BytesMut::from(bytes), version, &Limits::default())
}

/// The version to decode a packet with, which for CONNECT is not yet known.
fn context(bytes: &[u8], version: Version) -> Option<Version> {
    (bytes[0] >> 4 != 1).then_some(version)
}

#[test]
fn what_the_encoder_writes_the_decoder_reads() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let (mut written, mut refused) = (0usize, 0usize);
    for i in 0..60_000 {
        let (packet, version) = random_packet(&mut rng, i % 2 == 0);
        let Ok(bytes) = packet.to_bytes(version) else {
            refused += 1;
            continue;
        };
        written += 1;
        assert_eq!(
            packet.encoded_len(version).unwrap(),
            bytes.len(),
            "{packet:?}"
        );
        let back = decode(&bytes, context(&bytes, version))
            .unwrap_or_else(|e| panic!("{e} decoding {packet:?} from {bytes:02x?}"))
            .expect("whole packet");
        assert_eq!(back, packet, "bytes {bytes:02x?}");
    }
    // The generator has to produce packets the encoder accepts, or this proves nothing.
    assert!(
        written > 20_000,
        "only {written} written, {refused} refused"
    );
}

#[test]
fn damaged_input_never_panics_and_what_survives_round_trips() {
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    let mut accepted = 0usize;
    for i in 0..4_000 {
        let v5 = i % 2 == 0;
        let (packet, version) = random_packet(&mut rng, v5);
        let Ok(original) = packet.to_bytes(version) else {
            continue;
        };
        for _ in 0..12 {
            let mut bytes = original.to_vec();
            match rng.below(5) {
                0 => {
                    let at = rng.below(bytes.len());
                    bytes[at] ^= 1 << rng.below(8);
                }
                1 => {
                    let at = rng.below(bytes.len());
                    bytes[at] = rng.next() as u8;
                }
                2 => bytes.truncate(rng.below(bytes.len())),
                3 => bytes.extend_from_slice(&rng.bytes(8)),
                _ => {
                    let at = rng.below(bytes.len() + 1);
                    bytes.insert(at, rng.next() as u8);
                }
            }
            if bytes.is_empty() {
                continue;
            }
            for ctx in [Some(version), None] {
                let Ok(Some(damaged)) = decode(&bytes, ctx) else {
                    continue;
                };
                accepted += 1;
                // A CONNECT carries its own version; anything else uses the connection's.
                let v = match &damaged {
                    Packet::Connect(c) => c.version,
                    _ => version,
                };
                let again = damaged.to_bytes(v).unwrap_or_else(|e| {
                    panic!("accepted {bytes:02x?} as {damaged:?} but cannot write it: {e}")
                });
                let back = decode(&again, context(&again, v)).unwrap().unwrap();
                assert_eq!(back, damaged, "from {bytes:02x?}");
            }
        }
    }
    assert!(accepted > 1_000, "mutations rarely survived: {accepted}");
}

#[test]
fn arbitrary_bytes_never_panic() {
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    for _ in 0..200_000 {
        let n = rng.below(64);
        let mut bytes: Vec<u8> = (0..n).map(|_| rng.next() as u8).collect();
        // Make the first byte a plausible packet type so more inputs get past it.
        if let Some(first) = bytes.first_mut() {
            *first = (((rng.below(15) + 1) as u8) << 4) | (*first & 0x0F);
        }
        for ctx in [None, Some(Version::V311), Some(Version::V5)] {
            let _ = decode(&bytes, ctx);
        }
    }
}

#[test]
fn failed_encodes_leave_the_buffer_alone() {
    let mut dst = BytesMut::from(&b"keep"[..]);
    let bad = Packet::PubAck(Ack::new(0));
    assert!(bad.encode(Version::V5, &mut dst).is_err());
    assert_eq!(&dst[..], b"keep");
    // A property the packet cannot carry.
    let bad = Packet::Disconnect(Disconnect {
        reason: ReasonCode::Success,
        properties: Properties {
            receive_maximum: Some(0),
            ..Properties::default()
        },
    });
    assert!(bad.encode(Version::V5, &mut dst).is_err());
    assert_eq!(&dst[..], b"keep");
}

/// The fuzz target's seeds are well-formed packets; they stay that way, and the
/// target's properties hold for each of them.
#[test]
fn fuzz_seeds_decode_and_round_trip() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/seeds/mqtt_packet");
    let mut count = 0;
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let data = std::fs::read(&path).unwrap();
        let (selector, stream) = data.split_first().unwrap();
        let mut version = match selector % 3 {
            0 => None,
            1 => Some(Version::V311),
            _ => Some(Version::V5),
        };
        let mut src = BytesMut::from(stream);
        let mut packets = 0;
        while !src.is_empty() {
            let packet = Packet::decode(&mut src, version, &Limits::default())
                .unwrap_or_else(|e| panic!("{path:?}: {e}"))
                .unwrap_or_else(|| panic!("{path:?}: incomplete"));
            if let Packet::Connect(c) = &packet {
                version = Some(c.version);
            }
            let v = version.unwrap_or(Version::V311);
            let bytes = packet.to_bytes(v).unwrap();
            let back = decode(&bytes, context(&bytes, v)).unwrap().unwrap();
            assert_eq!(back, packet, "{path:?}");
            packets += 1;
        }
        assert!(packets > 0);
        count += 1;
    }
    assert!(count >= 60, "only {count} seeds");
}
