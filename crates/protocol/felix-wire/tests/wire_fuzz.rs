//! Deterministic fuzzing of the protocol decoders.
//!
//! The same properties the libFuzzer targets in `fuzz/` assert, driven by a
//! seeded generator so they run in the normal test suite and reproduce exactly.
//! The fuzz targets explore far more inputs; this file makes sure a regression
//! in the obvious ones cannot land without CI noticing.
//!
//! What makes this different from the storage fuzzing next door: those bytes
//! are written by Felix, and the question is whether recovery lies about them.
//! These bytes are written by whoever connects, and the question is whether
//! anything gets past the decoder that should not have.
//!
//! 1. **No panic, ever.** A hostile frame must produce a typed error, not an
//!    abort. This decoder is reached before authentication.
//! 2. **No unbounded allocation.** A count or length field is a number from a
//!    stranger until it has been checked against the bytes actually present.
//! 3. **No silent acceptance.** An unknown flag bit or message kind selects a
//!    payload layout, so ignoring one means confidently misparsing the body.
//! 4. **Round-trip fidelity.** Anything that decodes re-encodes to the bytes it
//!    came from — two byte sequences mapping to one message would mean the
//!    length prefix is not the only thing deciding where a message ends.
//!
//! Run the extended fuzzers with:
//!
//! ```text
//! cargo +nightly fuzz run frame            -- -max_total_time=300
//! cargo +nightly fuzz run internal_message -- -max_total_time=300
//! ```
use bytes::Bytes;
use felix_wire::binary;
use felix_wire::internal::{InternalHeader, InternalMessage, Kind};
use felix_wire::{
    FLAG_BINARY_EVENT_BATCH, FLAG_BINARY_EVENT_BATCH_SHARED, FLAG_BINARY_PUBLISH_ACK,
    FLAG_BINARY_PUBLISH_ACK_OFFSET, FLAG_BINARY_PUBLISH_ACK_OWNER, FLAG_BINARY_PUBLISH_ACKED,
    FLAG_BINARY_PUBLISH_BATCH, FLAG_BINARY_PUBLISH_KEYED, FLAG_EVENT_BATCH_OFFSETS, Frame,
    FrameHeader, KNOWN_FLAGS, MAGIC, Message, VERSION, has_unknown_flags,
};

/// xorshift64*. Deterministic and dependency-free, so a failing seed printed in
/// an assertion reproduces the exact byte sequence that broke.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            0
        } else {
            (self.next_u64() % bound as u64) as usize
        }
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next_u64() as u8).collect()
    }

    /// `bytes(below(bound))` as one call, so the borrow checker does not force
    /// a temporary at every use site.
    fn bytes_below(&mut self, bound: usize) -> Vec<u8> {
        let len = self.below(bound);
        self.bytes(len)
    }
}

/// The properties `fuzz_targets/frame.rs` asserts, for one input.
fn check_frame(data: &[u8], seed: u64) {
    let Ok(frame) = Frame::decode(Bytes::copy_from_slice(data)) else {
        return;
    };

    assert_eq!(
        frame.payload.len(),
        frame.header.length as usize,
        "seed {seed}: the payload is not the length the header declared, so a \
         reader would look for the next frame in the wrong place",
    );
    assert!(FrameHeader::LEN + frame.payload.len() <= data.len());
    assert_eq!(
        frame.encode().as_ref(),
        &data[..FrameHeader::LEN + frame.payload.len()],
        "seed {seed}: a frame did not re-encode to the bytes it came from",
    );
    assert!(
        !has_unknown_flags(frame.header.flags),
        "seed {seed}: a frame with an unknown flag bit decoded",
    );
}

/// A well-formed frame header in front of `payload`, so the generator spends
/// its inputs on bodies rather than on guessing twelve bytes of preamble.
fn framed(flags: u16, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(FrameHeader::LEN + payload.len());
    out.extend_from_slice(&MAGIC.to_be_bytes());
    out.extend_from_slice(&VERSION.to_be_bytes());
    out.extend_from_slice(&flags.to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

#[test]
fn a_frame_decodes_to_exactly_what_was_on_the_wire_or_not_at_all() {
    for seed in 0..2_000 {
        let mut rng = Rng::new(seed);
        // Raw noise: almost all of it fails on the magic, which is the point —
        // it is the cheapest possible refusal.
        let noise = rng.bytes_below(64);
        check_frame(&noise, seed);

        // A real header with a body, which is where the interesting cases are.
        let flags = rng.next_u64() as u16;
        let body = rng.bytes_below(48);
        check_frame(&framed(flags, &body), seed);

        // The same frame with its declared length corrupted: the decoder must
        // refuse rather than read past the buffer or hand back a short payload.
        let mut lying = framed(flags, &body);
        let bad_len = (rng.next_u64() as u32).to_be_bytes();
        lying[8..12].copy_from_slice(&bad_len);
        check_frame(&lying, seed);
    }
}

#[test]
fn a_length_larger_than_the_buffer_is_refused_rather_than_allocated() {
    // The header claims four gigabytes and carries one byte. A decoder that
    // sized anything from that number before checking it would die here.
    let mut framed = framed(0, b"x");
    framed[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(Frame::decode(Bytes::from(framed)).is_err());
}

#[test]
fn an_unknown_flag_bit_is_refused_on_every_frame_that_carries_one() {
    // Every bit outside the known mask, one at a time, alone and alongside a
    // known one: a reader that skipped its own check must still not get a body
    // it would misparse.
    for bit in 0..16u16 {
        let flags = 1u16 << bit;
        if flags & KNOWN_FLAGS != 0 {
            continue;
        }
        for with in [0, FLAG_BINARY_PUBLISH_BATCH] {
            assert!(
                Frame::decode(Bytes::from(framed(flags | with, b"body"))).is_err(),
                "flag {:#06x} is outside KNOWN_FLAGS but decoded",
                flags | with,
            );
        }
    }
}

#[test]
fn every_binary_layout_refuses_a_body_it_cannot_account_for() {
    const LAYOUTS: &[u16] = &[
        FLAG_BINARY_PUBLISH_BATCH,
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_KEYED,
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED,
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED | FLAG_BINARY_PUBLISH_KEYED,
        FLAG_BINARY_PUBLISH_ACK,
        FLAG_BINARY_PUBLISH_ACK | FLAG_BINARY_PUBLISH_ACK_OFFSET,
        FLAG_BINARY_PUBLISH_ACK | FLAG_BINARY_PUBLISH_ACK_OWNER | FLAG_BINARY_PUBLISH_ACK_OFFSET,
        FLAG_BINARY_EVENT_BATCH,
        FLAG_BINARY_EVENT_BATCH | FLAG_EVENT_BATCH_OFFSETS,
        FLAG_BINARY_EVENT_BATCH_SHARED,
        FLAG_BINARY_EVENT_BATCH_SHARED | FLAG_EVENT_BATCH_OFFSETS,
    ];

    for seed in 0..2_000 {
        let mut rng = Rng::new(seed);
        let body = rng.bytes_below(64);
        for &flags in LAYOUTS {
            let frame = Frame::new(flags, Bytes::from(body.clone())).expect("frame");

            // Property: parse or error, never panic, and never report more
            // payloads than the body had bytes to describe them with.
            if flags & FLAG_BINARY_PUBLISH_ACKED != 0 {
                if let Ok(batch) = binary::decode_acked_publish_batch(&frame) {
                    assert!(batch.batch.payloads.len() <= body.len(), "seed {seed}");
                }
            } else if flags & FLAG_BINARY_PUBLISH_BATCH != 0 {
                if let Ok(batch) = binary::decode_publish_batch(&frame) {
                    assert!(batch.payloads.len() <= body.len(), "seed {seed}");
                }
            } else if flags & FLAG_BINARY_PUBLISH_ACK != 0 {
                let _ = binary::decode_publish_ack(&frame);
            } else if flags & FLAG_BINARY_EVENT_BATCH_SHARED != 0 {
                if let Ok(batch) = binary::decode_shared_event_batch(&frame) {
                    assert!(batch.payloads.len() <= body.len(), "seed {seed}");
                }
            } else if let Ok(batch) = binary::decode_event_batch(&frame) {
                assert!(batch.payloads.len() <= body.len(), "seed {seed}");
            }
        }
    }
}

#[test]
fn a_publish_batch_claiming_more_payloads_than_it_carries_is_refused() {
    // tenant, namespace, stream, then a count of four billion payloads and
    // nothing to back it. The count must be checked against the remaining
    // bytes before it is used to reserve anything.
    let mut body = Vec::new();
    for part in [b"t".as_slice(), b"n".as_slice(), b"s".as_slice()] {
        body.extend_from_slice(&(part.len() as u32).to_be_bytes());
        body.extend_from_slice(part);
    }
    body.extend_from_slice(&u32::MAX.to_be_bytes());

    let frame = Frame::new(FLAG_BINARY_PUBLISH_BATCH, Bytes::from(body)).expect("frame");
    assert!(binary::decode_publish_batch(&frame).is_err());
}

/// The properties `fuzz_targets/internal_message.rs` asserts, for one input.
fn check_internal(data: &[u8], seed: u64) {
    let Ok(message) = InternalMessage::decode(Bytes::copy_from_slice(data)) else {
        return;
    };

    let re_encoded = message.encode().unwrap_or_else(|err| {
        panic!("seed {seed}: a message that decoded would not re-encode: {err}")
    });
    assert_eq!(
        re_encoded.as_ref(),
        data,
        "seed {seed}: an internal message did not re-encode to the bytes it \
         came from, so trailing bytes were ignored rather than refused",
    );
    let again = InternalMessage::decode(re_encoded).expect("re-decode");
    assert_eq!(again, message, "seed {seed}: decoding is not deterministic");
}

#[test]
fn an_internal_message_decodes_to_exactly_what_was_on_the_wire_or_not_at_all() {
    for seed in 0..4_000 {
        let mut rng = Rng::new(seed);
        let noise = rng.bytes_below(96);
        check_internal(&noise, seed);

        // Mutate a real message: the decoder's interesting failures are one bit
        // away from something valid, not in uniform noise.
        let hello = InternalMessage::Hello(felix_wire::internal::Hello {
            correlation_id: rng.next_u64(),
            node_id: "broker-a".to_string(),
            capabilities: Some(felix_wire::internal::PeerCapabilities::FENCE),
        });
        let mut bytes = hello.encode().expect("encode").to_vec();
        if !bytes.is_empty() {
            let at = rng.below(bytes.len());
            bytes[at] ^= 1u8 << rng.below(8);
        }
        check_internal(&bytes, seed);

        // A truncation at every length, which is what a half-delivered frame
        // looks like to the decoder.
        let full = hello.encode().expect("encode");
        let cut = rng.below(full.len());
        check_internal(&full[..cut], seed);
    }
}

#[test]
fn an_unknown_internal_kind_is_refused_rather_than_skipped() {
    // The kind selects how to read the body, so one that is merely ignored is
    // one whose body gets read as something else's.
    for raw in 0..1024u16 {
        if let Ok(kind) = Kind::from_u16(raw) {
            assert_eq!(kind as u16, raw);
        }
    }
    assert!(Kind::from_u16(0).is_err());
    assert!(Kind::from_u16(u16::MAX).is_err());
}

#[test]
fn an_internal_body_longer_than_the_limit_is_refused_before_it_is_allocated() {
    // The header is the only thing read before the body's size is trusted, so
    // this is the one place a stranger's number could reserve memory.
    let mut header = Vec::new();
    header.extend_from_slice(&felix_wire::internal::INTERNAL_MAGIC.to_be_bytes());
    header.extend_from_slice(&felix_wire::internal::INTERNAL_VERSION.to_be_bytes());
    header.extend_from_slice(&(Kind::Hello as u16).to_be_bytes());
    header.extend_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(header.len(), InternalHeader::LEN);

    assert!(InternalMessage::decode(Bytes::from(header)).is_err());
}

#[test]
fn a_control_message_survives_the_round_trip_it_is_dispatched_on() {
    for seed in 0..2_000 {
        let mut rng = Rng::new(seed);
        let body = rng.bytes_below(64);
        let frame = Frame::new(0, Bytes::from(body)).expect("frame");

        let Ok(message) = Message::decode(frame) else {
            continue;
        };
        // An optional field that did not survive is one the broker would act on
        // differently from what the client asked for.
        let encoded = message.encode().expect("a decoded message re-encodes");
        assert_eq!(
            Message::decode(encoded).expect("re-decode"),
            message,
            "seed {seed}: a control message changed meaning across a round trip",
        );
    }
}
