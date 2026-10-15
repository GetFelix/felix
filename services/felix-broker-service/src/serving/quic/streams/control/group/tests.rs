use bytes::Bytes;
use felix_wire::{GroupRecord, Message};

use super::for_peer;

fn polled() -> Vec<GroupRecord> {
    vec![GroupRecord {
        offset: 7,
        payload: Bytes::from_static(b"v"),
        attempts: 1,
        skipped_before: 2,
        publisher: Some("alice".to_string()),
        timestamp_micros: Some(1_700_000_000_000_000),
    }]
}

fn frame(records: Vec<GroupRecord>) -> String {
    let message = Message::GroupRecords {
        records,
        request_id: 1,
    };
    String::from_utf8(message.encode().expect("encode").payload.to_vec()).expect("utf8")
}

/// **A client that never negotiated `skipped_before` does not get it**, even
/// when the broker has a count to report, so its frame is the one it always
/// got.
#[test]
fn a_client_without_the_feature_gets_records_without_skipped_before() {
    let records = for_peer(
        polled(),
        felix_wire::KNOWN_FEATURES & !felix_wire::FEATURE_GROUP_SKIPPED,
    );
    assert_eq!(records[0].skipped_before, 0);
    assert!(!frame(records).contains("skipped_before"));
    assert!(!frame(for_peer(polled(), 0)).contains("skipped_before"));
}

#[test]
fn a_client_with_the_feature_gets_skipped_before() {
    let records = for_peer(polled(), felix_wire::FEATURE_GROUP_SKIPPED);
    assert_eq!(records[0].skipped_before, 2);
    assert!(frame(records).contains("\"skipped_before\":2"));
}

/// The publisher goes only to a client that asked: any other gets the frame
/// it always got.
#[test]
fn only_a_client_with_the_feature_gets_the_publisher() {
    let without = for_peer(
        polled(),
        felix_wire::KNOWN_FEATURES & !felix_wire::FEATURE_GROUP_PUBLISHER,
    );
    assert_eq!(without[0].publisher, None);
    assert!(!frame(without).contains("publisher"));

    let with = for_peer(polled(), felix_wire::FEATURE_GROUP_PUBLISHER);
    assert_eq!(with[0].publisher.as_deref(), Some("alice"));
    assert!(frame(with).contains("\"publisher\":\"alice\""));
}

/// The append time goes only to a client that asked. A client that offered
/// none of the bits gets exactly the frame a broker before them sent.
#[test]
fn only_a_client_with_the_feature_gets_the_time() {
    let without = for_peer(
        polled(),
        felix_wire::KNOWN_FEATURES & !felix_wire::FEATURE_RECORD_TIMESTAMPS,
    );
    assert_eq!(without[0].timestamp_micros, None);
    assert!(!frame(without).contains("timestamp_micros"));
    assert_eq!(
        frame(for_peer(polled(), 0)),
        r#"{"type":"group_records","records":[{"offset":7,"payload":"dg==","attempts":1}],"request_id":1}"#
    );

    let with = for_peer(polled(), felix_wire::FEATURE_RECORD_TIMESTAMPS);
    assert_eq!(with[0].timestamp_micros, Some(1_700_000_000_000_000));
    assert!(frame(with).contains("\"timestamp_micros\":1700000000000000"));
}
