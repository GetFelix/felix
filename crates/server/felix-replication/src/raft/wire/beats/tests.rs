use bytes::Bytes;
use std::time::Duration;

use super::*;

#[test]
fn heartbeats_and_their_answers_keep_their_order() {
    let beats = [
        Bytes::from_static(b"one"),
        Bytes::new(),
        Bytes::from_static(b"three"),
    ];
    let body = encode(Duration::from_millis(250), &beats).expect("encode");
    assert_eq!(
        crate::peer::raft_deadline(&body),
        Duration::from_millis(250)
    );
    let (deadline, decoded) = decode(body).expect("decode");
    assert_eq!(deadline, Duration::from_millis(250));
    assert_eq!(decoded, beats);

    let answers = [Some(Bytes::from_static(b"a")), None, Some(Bytes::new())];
    let decoded = decode_answers(encode_answers(&answers), 3).expect("decode");
    assert_eq!(decoded, answers);
}

#[test]
fn answers_that_do_not_match_the_heartbeats_are_refused() {
    let answers = encode_answers(&[Some(Bytes::from_static(b"a")), None]);
    assert!(decode_answers(answers.clone(), 1).is_err());
    assert!(decode_answers(answers.clone(), 3).is_err());
    assert!(decode_answers(answers.slice(..answers.len() - 1), 2).is_err());
}

#[test]
fn a_frame_holds_at_most_max_beats() {
    let beats = vec![Bytes::new(); MAX_BEATS + 1];
    assert!(encode(Duration::from_millis(1), &beats).is_err());
    let mut forged = encode(Duration::from_millis(1), &beats[..MAX_BEATS])
        .expect("encode")
        .to_vec();
    forged[6..8].copy_from_slice(&((MAX_BEATS + 1) as u16).to_be_bytes());
    forged.extend_from_slice(&[0, 0, 0, 0]);
    assert!(decode(Bytes::from(forged)).is_err());
}

#[test]
fn a_body_longer_than_the_frame_is_refused() {
    let mut forged = encode(Duration::from_millis(1), &[Bytes::from_static(b"abc")])
        .expect("encode")
        .to_vec();
    forged[8..12].copy_from_slice(&100u32.to_be_bytes());
    assert!(decode(Bytes::from(forged)).is_err());
}
