use std::sync::Arc;

use bytes::Bytes;
use felix_wire::{Frame, binary};

use super::{DeliveryEnvelope, FrameShape};

const ALL: FrameShape = FrameShape {
    offsets: true,
    skips: true,
    publisher: true,
    headers: false,
    timestamps: true,
};

fn decode(frame: Bytes) -> binary::SharedEventBatch {
    binary::decode_shared_event_batch(&Frame::decode(frame).expect("frame")).expect("batch")
}

/// A subscriber that asked for the publisher gets it on the shared frame;
/// one that did not gets the frame it always got.
#[test]
fn the_publisher_rides_only_the_frames_that_asked_for_it() {
    let payloads = [Bytes::from_static(b"a")];
    let envelope = DeliveryEnvelope::published(
        &payloads,
        Some(7),
        0,
        Some(Bytes::from_static(b"alice")),
        None,
        None,
    );
    let batch = decode(envelope.shared_event_frame_as(ALL).expect("encode"));
    assert_eq!(
        (batch.base_offset, batch.publisher.as_deref()),
        (Some(7), Some(&b"alice"[..]))
    );
    assert_eq!(
        envelope
            .shared_event_frame_as(FrameShape {
                offsets: true,
                ..FrameShape::default()
            })
            .expect("encode"),
        binary::encode_shared_event_batch_bytes_with_offset(&payloads, 7).expect("encode")
    );
    // Encoded once and shared.
    assert_eq!(
        envelope
            .shared_event_frame_as(ALL)
            .expect("encode")
            .as_ptr(),
        envelope
            .shared_event_frame_as(ALL)
            .expect("encode")
            .as_ptr()
    );
}

/// A batch with no recorded publisher is the plainer frame, even for a
/// subscriber that asked.
#[test]
fn no_publisher_is_the_frame_without_one() {
    let payloads = [Bytes::from_static(b"a")];
    let envelope = DeliveryEnvelope::published(&payloads, None, 0, None, None, None);
    assert_eq!(
        envelope.shared_event_frame_as(ALL).expect("encode"),
        envelope.shared_event_frame().expect("encode")
    );
}

/// Every record of a live batch carries the one time it was appended with,
/// and only for a subscriber that asked; the others share the frame they
/// always got.
#[test]
fn the_time_rides_only_the_frames_that_asked_for_it() {
    let payloads = [Bytes::from_static(b"a"), Bytes::from_static(b"b")];
    let envelope = DeliveryEnvelope::published(&payloads, Some(3), 0, None, Some(42), None);
    let batch = decode(envelope.shared_event_frame_as(ALL).expect("encode"));
    assert_eq!(batch.timestamps.as_deref(), Some(&[42, 42][..]));
    assert_eq!(batch.base_offset, Some(3));
    let offsets_only = envelope.shared_event_frame_with_offsets().expect("encode");
    assert_eq!(
        offsets_only,
        binary::encode_shared_event_batch_bytes_with_offset(&payloads, 3).expect("encode")
    );
    // A later slice keeps the time.
    let rest = decode(
        envelope
            .skip_records(1)
            .shared_event_frame_as(ALL)
            .expect("encode"),
    );
    assert_eq!(rest.timestamps.as_deref(), Some(&[42][..]));
    // An in-memory batch has no time to send.
    let memory = DeliveryEnvelope::published(&payloads, None, 0, None, None, None);
    assert_eq!(
        memory.shared_event_frame_as(ALL).expect("encode"),
        memory.shared_event_frame().expect("encode")
    );
}

fn blocks(values: &[Option<&'static [u8]>]) -> Arc<[Option<Bytes>]> {
    values
        .iter()
        .map(|block| block.map(Bytes::from_static))
        .collect()
}

const HEADERS: FrameShape = FrameShape {
    offsets: true,
    skips: false,
    publisher: false,
    headers: true,
    timestamps: false,
};

fn with_headers() -> (DeliveryEnvelope, [Bytes; 3]) {
    // Whole blocks: no key, one header `a: 1`.
    const BLOCK: &[u8] = &[0, 0, 0, 0, 1, 0, 1, b'a', 0, 0, 0, 1, b'1'];
    let payloads = [
        Bytes::from_static(b"x"),
        Bytes::from_static(b"y"),
        Bytes::from_static(b"z"),
    ];
    let envelope = DeliveryEnvelope::published(
        &payloads,
        Some(10),
        0,
        None,
        None,
        Some(blocks(&[None, Some(BLOCK), Some(BLOCK)])),
    );
    (envelope, payloads)
}

/// A subscriber that negotiated headers gets each record's block, aligned
/// with its payload; one that did not gets the frame a batch without headers
/// would have made, byte for byte.
#[test]
fn headers_ride_only_the_frames_that_negotiated_them() {
    let (envelope, payloads) = with_headers();
    let batch = decode(envelope.shared_event_frame_as(HEADERS).expect("encode"));
    let sent = batch.record_headers.expect("headers");
    assert_eq!(sent.len(), 3);
    assert!(sent[0].is_none() && sent[1].is_some() && sent[2].is_some());
    assert_eq!(batch.payloads, payloads);

    let offsets_only = FrameShape {
        headers: false,
        ..HEADERS
    };
    assert_eq!(
        envelope
            .shared_event_frame_as(offsets_only)
            .expect("encode"),
        DeliveryEnvelope::published(&payloads, Some(10), 0, None, None, None)
            .shared_event_frame_as(offsets_only)
            .expect("encode")
    );
    assert_eq!(
        envelope
            .shared_event_frame_as(FrameShape::default())
            .expect("encode"),
        binary::encode_shared_event_batch_bytes(&payloads).expect("encode")
    );
}

/// Each shape is encoded once however many subscribers ask, and the frames
/// with and without headers are cached apart.
#[test]
fn each_shape_with_headers_is_encoded_once() {
    let (envelope, _) = with_headers();
    let with = envelope.shared_event_frame_as(HEADERS).expect("encode");
    let without = envelope
        .shared_event_frame_as(FrameShape {
            headers: false,
            ..HEADERS
        })
        .expect("encode");
    assert_ne!(with, without);
    assert_eq!(
        with.as_ptr(),
        envelope
            .shared_event_frame_as(HEADERS)
            .expect("encode")
            .as_ptr()
    );
    assert_eq!(
        without.as_ptr(),
        envelope
            .shared_event_frame_as(FrameShape {
                headers: false,
                ..HEADERS
            })
            .expect("encode")
            .as_ptr()
    );
}

/// A headers subscriber on a batch where no record has one gets the plain
/// frame: nothing extra is sent or cached for it.
#[test]
fn a_batch_without_headers_is_one_frame_for_every_shape() {
    let payloads = [Bytes::from_static(b"x")];
    let envelope = DeliveryEnvelope::published(&payloads, Some(10), 0, None, None, None);
    let plain = FrameShape {
        headers: false,
        ..HEADERS
    };
    assert_eq!(
        envelope.shared_event_frame_as(HEADERS).expect("encode"),
        envelope.shared_event_frame_as(plain).expect("encode")
    );
    assert!(envelope.record_headers().is_none());
    assert!(envelope.inner.encoded_headers.get().is_none());
}

/// Skipping records keeps the blocks lined up with the payloads that are left,
/// and drops the headers altogether once none of them has one.
#[test]
fn skip_records_keeps_headers_aligned() {
    let (envelope, _) = with_headers();
    let rest = envelope.skip_records(1);
    assert_eq!(rest.base_offset(), Some(11));
    let kept = rest.record_headers().expect("headers");
    assert_eq!(kept.len(), 2);
    assert!(kept.iter().all(Option::is_some));
    let batch = decode(rest.shared_event_frame_as(HEADERS).expect("encode"));
    assert_eq!(batch.record_headers.expect("headers").len(), 2);

    let payloads = [Bytes::from_static(b"x"), Bytes::from_static(b"y")];
    let only_first = DeliveryEnvelope::published(
        &payloads,
        Some(0),
        0,
        None,
        None,
        Some(blocks(&[
            Some(&[0, 0, 0, 0, 1, 0, 1, b'a', 0, 0, 0, 1, b'1']),
            None,
        ])),
    );
    assert!(only_first.skip_records(1).record_headers().is_none());
}
