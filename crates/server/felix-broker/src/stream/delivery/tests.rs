use bytes::Bytes;
use felix_wire::{Frame, binary};

use super::{DeliveryEnvelope, FrameShape};

const ALL: FrameShape = FrameShape {
    offsets: true,
    skips: true,
    publisher: true,
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
    let envelope = DeliveryEnvelope::published(&payloads, None, 0, None, None);
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
    let envelope = DeliveryEnvelope::published(&payloads, Some(3), 0, None, Some(42));
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
    let memory = DeliveryEnvelope::published(&payloads, None, 0, None, None);
    assert_eq!(
        memory.shared_event_frame_as(ALL).expect("encode"),
        memory.shared_event_frame().expect("encode")
    );
}
