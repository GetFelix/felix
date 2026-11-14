//! The broker-to-broker decoder against arbitrary bytes.
//!
//! A peer connection is authenticated, but "authenticated" is not "trusted with
//! a length field": a compromised or simply mismatched broker reaches this
//! decoder with whatever it likes, and an undecodable frame here ends a
//! long-lived lane carrying every in-flight request.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use felix_wire::internal::{
    FrameEnvelope, InternalHeader, InternalMessage, Kind, PeerCapabilities, correlation_id_in,
};

fuzz_target!(|data: &[u8]| {
    // Property 1: an unknown kind is rejected, never skipped. The kind selects
    // how to read the body, so ignoring one means misparsing it.
    if data.len() >= 2 {
        let raw = u16::from_be_bytes([data[0], data[1]]);
        if let Ok(kind) = Kind::from_u16(raw) {
            assert_eq!(kind as u16, raw);
        }
    }

    // The peer reader steps over a kind it does not know using only the
    // envelope, so the envelope must agree with the full header whenever both
    // read the same bytes.
    let input = Bytes::copy_from_slice(data);
    if let Ok(header) = InternalHeader::decode(&input) {
        let envelope = FrameEnvelope::decode(&input).expect("header decoded");
        assert_eq!(envelope.kind, header.kind as u16);
        assert_eq!(envelope.length, header.length);
    }
    let _ = correlation_id_in(input.get(InternalHeader::LEN..).unwrap_or_default());

    // Property 2: arbitrary bytes decode or error, never panic, and never
    // allocate on the strength of an unvalidated length.
    let Ok(message) = InternalMessage::decode(Bytes::copy_from_slice(data)) else {
        return;
    };

    // Property 3: what decoded re-encodes to the same bytes. The decoder
    // rejects trailing bytes rather than ignoring them, and this is what holds
    // that true — a body with slack would re-encode shorter than it arrived.
    let Ok(re_encoded) = message.encode() else {
        panic!("a message that decoded could not be re-encoded");
    };
    assert_eq!(re_encoded.as_ref(), data);

    // Property 4: a message needs a capability only if its kind is listed in
    // `capability_rules`: `required_capability` must include every `must` bit
    // and nothing outside `must | may`. A kind that is not listed needs
    // nothing, so a new negotiated kind fails here until it gets a row.
    // Nothing else may be withheld from a peer that did not offer it.
    // An incarnation envelope can hold any of them, so it is not checked.
    if let Some(header) = InternalHeader::decode(&input)
        .ok()
        .filter(|h| h.kind != Kind::Incarnated)
    {
        let (must, may) = capability_rules(header.kind);
        let required = message.required_capability().unwrap_or(PeerCapabilities::NONE);
        assert!(
            required.contains(must) && must.union(may).contains(required),
            "{:?} requires {:#x}, expected {:#x} plus at most {:#x}; add the kind to capability_rules",
            header.kind,
            required.bits(),
            must.bits(),
            may.bits()
        );
    }

    // Property 5: the round trip is stable, so decoding is a function of the
    // bytes rather than of anything left over from the first pass.
    let again = InternalMessage::decode(re_encoded).expect("re-decode");
    assert_eq!(again, message);
});

/// The capabilities a decoded message of this kind must report, and the extra
/// ones it may report depending on its body.
fn capability_rules(kind: Kind) -> (PeerCapabilities, PeerCapabilities) {
    use PeerCapabilities as C;
    let none = C::NONE;
    match kind {
        Kind::ElectionKeptFence => (C::ELECTION.union(C::KEPT_SETS), none),
        Kind::ElectionFence | Kind::PreVote | Kind::PreVoteAnswer => (C::ELECTION, none),
        Kind::Raft | Kind::RaftAnswer => (C::RAFT, none),
        // A stamped batch needs ELECTION, and the headered or writer-stamped
        // blocks inside add their own.
        Kind::SetStamped => (C::ELECTION, C::RECORD_HEADERS.union(C::WRITER_GENERATIONS)),
        Kind::ReplicateHeaderedRecords
        | Kind::ReplicateHeaderedFetch
        | Kind::HeaderedForwardPublish => (C::RECORD_HEADERS, none),
        Kind::SequencedForwardPublish => (C::RECORD_HEADERS.union(C::SEQUENCED_FORWARDS), none),
        // Writer runs; a set generation adds ELECTION.
        Kind::ReplicateStampedRecords => (C::WRITER_GENERATIONS, C::ELECTION),
        Kind::ReplicateStampedFetch => (C::WRITER_GENERATIONS, none),
        Kind::SealFence | Kind::SealAck => (C::SWITCH_SEAL, none),
        // Plus whatever the request inside needs.
        Kind::FromCopy => (
            C::SWITCH_SEAL,
            C::ELECTION
                .union(C::KEPT_SETS)
                .union(C::RAFT)
                .union(C::RECORD_HEADERS)
                .union(C::SEQUENCED_FORWARDS)
                .union(C::WRITER_GENERATIONS),
        ),
        _ => (none, none),
    }
}
