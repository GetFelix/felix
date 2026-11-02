use bytes::Bytes;
use felix_wire::RecordHeaders;

use super::publishable;

fn block(names: &[&str]) -> Bytes {
    RecordHeaders {
        key: None,
        headers: names
            .iter()
            .map(|name| ((*name).to_owned(), Bytes::from_static(b"v")))
            .collect(),
    }
    .encode()
    .expect("block")
}

#[test]
fn a_batch_without_blocks_is_the_plain_publish() {
    assert!(publishable(None).expect("ok").is_none());
    assert!(publishable(Some(vec![None, None])).expect("ok").is_none());
}

#[test]
fn blocks_pass_through_aligned_with_the_records() {
    let first = block(&["trace", "type"]);
    let headers = publishable(Some(vec![Some(first.clone()), None]))
        .expect("ok")
        .expect("some");
    assert_eq!(headers, vec![Some(first), None]);
}

#[test]
fn a_reserved_name_in_any_record_refuses_the_batch() {
    let refused = publishable(Some(vec![
        Some(block(&["trace"])),
        None,
        Some(block(&["ok", "felix.mqtt.topic"])),
    ]))
    .expect_err("reserved");
    assert_eq!(refused.code(), &felix_wire::ErrorCode::InvalidRequest);
    // A look-alike is not reserved.
    assert!(publishable(Some(vec![Some(block(&["felixa", "Felix.x"]))])).is_ok());
}
