use bytes::Bytes;

use super::RecordHeaders;

fn wire() -> felix_client::RecordHeaders {
    felix_client::RecordHeaders {
        key: Some(Bytes::from_static(b"own")),
        headers: vec![
            ("tag".to_string(), Bytes::from_static(b"a")),
            ("tag".to_string(), Bytes::from_static(b"b")),
            ("empty".to_string(), Bytes::new()),
        ],
    }
}

#[test]
fn a_block_survives_the_trip_through_python_form_and_back() {
    let back = RecordHeaders::from(wire()).to_client();
    assert_eq!(back, wire());
}

#[test]
fn repeated_names_keep_their_order() {
    let names: Vec<_> = RecordHeaders::from(wire())
        .to_client()
        .headers
        .into_iter()
        .map(|(name, value)| (name, value.to_vec()))
        .collect();
    assert_eq!(names[0], ("tag".to_string(), b"a".to_vec()));
    assert_eq!(names[1], ("tag".to_string(), b"b".to_vec()));
}

#[test]
fn a_block_with_neither_key_nor_headers_converts_to_an_empty_one() {
    let empty = RecordHeaders::from(felix_client::RecordHeaders::default()).to_client();
    assert!(empty.is_empty());
}
