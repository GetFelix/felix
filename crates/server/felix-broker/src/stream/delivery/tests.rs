use bytes::Bytes;
use felix_wire::{Frame, binary};

use super::{DeliveryEnvelope, FrameShape};

const ALL: FrameShape = FrameShape {
    offsets: true,
    skips: true,
    publisher: true,
};

fn decode(frame: Bytes) -> binary::SharedEventBatch {
    binary::decode_shared_event_batch(&Frame::decode(frame).expect("frame")).expect("batch")
}

/// A subscriber that asked for the publisher gets it on the shared frame;
/// one that did not gets the frame it always got.
#[test]
fn the_publisher_rides_only_the_frames_that_asked_for_it() {
    let payloads = [Bytes::from_static(b"a")];
    let envelope =
        DeliveryEnvelope::published(&payloads, Some(7), 0, Some(Bytes::from_static(b"alice")));
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
    let envelope = DeliveryEnvelope::published(&payloads, None, 0, None);
    assert_eq!(
        envelope.shared_event_frame_as(ALL).expect("encode"),
        envelope.shared_event_frame().expect("encode")
    );
}
