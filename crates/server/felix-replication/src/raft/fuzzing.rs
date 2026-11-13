//! Entry points for the libFuzzer targets in `crates/server/felix-replication/fuzz`.
//!
//! Behind the `fuzzing` feature and not an API: they let the targets reach
//! the Raft message decoders without the crate exporting them. Each asserts
//! the properties its target checks, so a violation is a panic libFuzzer
//! records.

use bytes::Bytes;

use super::network::{RaftRequest, Request};
use super::wire;

/// Read a `Raft` frame's body the way a listener does.
///
/// These bytes come from an authenticated peer, which is not a peer trusted
/// with a count: they must decode or be refused, never panic or reserve on
/// the strength of a count the bytes cannot back.
pub fn raft_request(bytes: &[u8]) {
    let deadline = crate::peer::raft_deadline(bytes);
    let Ok(mut request) = wire::decode_request(Bytes::copy_from_slice(bytes)) else {
        return;
    };
    // The listener's bound is what the full decode reads, held to its limit.
    assert_eq!(
        deadline,
        request.deadline.min(crate::peer::RAFT_MAX_DEADLINE)
    );
    // What decoded encodes, and the encoding is stable. Not byte-identical to
    // the input: member sets are re-encoded in their own order.
    let encoded = wire::encode_request(&request).expect("a decoded message encodes");
    let again = wire::decode_request(encoded.clone()).expect("its encoding decodes");
    let re_encoded = wire::encode_request(&again).expect("and encodes again");
    assert_eq!(encoded, re_encoded);

    // A cut keeps at least one entry and never adds any.
    let entries = |request: &RaftRequest| match &request.body {
        Request::Append(rpc) => Some(rpc.entries.len()),
        _ => None,
    };
    let before = entries(&request);
    wire::fit(&mut request);
    match (before, entries(&request)) {
        (Some(0), Some(after)) => assert_eq!(after, 0),
        (Some(before), Some(after)) => assert!((1..=before).contains(&after)),
        (before, after) => assert_eq!(before, after),
    }
}

/// Read a `RaftAnswer` frame's body the way a sender does. It must decode or
/// be refused, and what decoded must re-encode to the same answer.
pub fn raft_answer(bytes: &[u8]) {
    let Ok(answer) = wire::decode_reply(Bytes::copy_from_slice(bytes)) else {
        return;
    };
    let encoded = wire::encode_reply(&answer);
    let again = wire::decode_reply(encoded.clone()).expect("its encoding decodes");
    assert_eq!(encoded, wire::encode_reply(&again));
}
