//! MQTT over WebSockets against arbitrary bytes.
//!
//! The first byte picks what the rest is: an upgrade request, or a stream of
//! client frames. The second byte, for frames, is how many bytes arrive per
//! read.

#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

use felix_mqtt::ws::{AllowedOrigins, Event, FrameDecoder, Settings, Violation, parse};

/// What decoding a stream came to: the payload, the control frames, and
/// whether a frame broke the format.
#[derive(Debug, PartialEq)]
struct Outcome {
    data: Vec<u8>,
    controls: Vec<Event>,
    violation: Option<Violation>,
}

fn decode(stream: &[u8], step: usize) -> Outcome {
    let mut decoder = FrameDecoder::new();
    let mut outcome = Outcome {
        data: Vec::new(),
        controls: Vec::new(),
        violation: None,
    };
    let (mut src, mut data) = (BytesMut::new(), BytesMut::new());
    for chunk in stream.chunks(step.max(1)) {
        src.extend_from_slice(chunk);
        loop {
            let appended = data.len();
            match decoder.next(&mut src, &mut data) {
                Ok(Some(Event::Data)) => {
                    outcome.data.extend_from_slice(&data.split());
                }
                Ok(Some(control)) => outcome.controls.push(control),
                // Waiting for more hands over no payload.
                Ok(None) => {
                    assert_eq!(appended, data.len());
                    break;
                }
                Err(violation) => {
                    outcome.violation = Some(violation);
                    return outcome;
                }
            }
        }
    }
    outcome
}

fuzz_target!(|input: &[u8]| {
    let Some((&selector, rest)) = input.split_first() else {
        return;
    };
    if selector % 2 == 0 {
        // Property 1: any request is accepted or refused, never a panic, and
        // an accepted key is the 24 characters the format asks for.
        let settings = Settings {
            allowed_origins: AllowedOrigins::Any,
            ..Settings::default()
        };
        if let Ok(key) = parse(rest, &settings) {
            assert_eq!(key.len(), 24);
        }
        return;
    }

    let Some((&step, stream)) = rest.split_first() else {
        return;
    };
    // Property 2: frames decode to the same payload and the same verdict
    // whether they arrive whole or a few bytes at a time.
    let whole = decode(stream, stream.len());
    let cut = decode(stream, usize::from(step));
    assert_eq!(whole, cut);
    // Payload never exceeds the bytes that carried it.
    assert!(whole.data.len() <= stream.len());
});
