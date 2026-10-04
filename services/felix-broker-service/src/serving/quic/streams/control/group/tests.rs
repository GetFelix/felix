use bytes::Bytes;
use felix_wire::{GroupRecord, Message};

use super::for_peer;

fn polled() -> Vec<GroupRecord> {
    vec![GroupRecord {
        offset: 7,
        payload: Bytes::from_static(b"v"),
        attempts: 1,
        skipped_before: 2,
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
