//! Array counts come from the client, so decoding must not reserve memory for
//! them before the bytes to back them have arrived.

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::MetadataRequest;
use kafka_protocol::protocol::Decodable;

#[test]
fn an_array_count_past_the_frame_is_refused_without_reserving_it() {
    // Metadata v1: a nullable topics array whose count claims ~1.7 billion
    // entries, followed by nothing.
    let mut body = BytesMut::new();
    body.put_i32(0x6666_6666);
    let mut body: Bytes = body.freeze();

    assert!(MetadataRequest::decode(&mut body, 1).is_err());
}

#[test]
fn a_compact_array_count_past_the_frame_is_refused_without_reserving_it() {
    // Metadata v9 is flexible: compact arrays carry count + 1 as an unsigned varint.
    let mut body = BytesMut::new();
    body.put_slice(&[0xff, 0xff, 0xff, 0xff, 0x07]);
    let mut body: Bytes = body.freeze();

    assert!(MetadataRequest::decode(&mut body, 9).is_err());
}

#[test]
fn a_record_count_past_the_batch_is_refused_without_reserving_it() {
    // Found by the `kafka_records` fuzz target: a v2 batch whose header claims
    // far more records than the 66 bytes could hold.
    let batch: &[u8] = &[
        0, 0, 0, 0, 0, 0, 0, 59, 0, 0, 0, 54, 255, 255, 255, 255, 2, 89, 183, 11, 47, 0, 0, 1, 139,
        207, 229, 104, 0, 0, 0, 1, 59, 0, 0, 0, 0, 0, 0, 1, 139, 207, 229, 104, 0, 0, 0, 1, 139,
        207, 104, 1, 4, 0, 3, 0, 0, 1, 14, 0, 2, 0, 1, 2, 56, 0,
    ];
    let mut bytes = Bytes::copy_from_slice(batch);
    // Decoding may fail; it must not try to reserve memory for the claimed count.
    let _ = kafka_protocol::records::RecordBatchDecoder::decode(&mut bytes);
}
