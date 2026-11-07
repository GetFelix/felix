use bytes::Bytes;
use napi::bindgen_prelude::Buffer;

use super::{HeaderField, RecordHeaders, RecordHeadersInput};

fn field(name: &str, value: &[u8]) -> HeaderField {
    HeaderField {
        name: name.to_string(),
        value: Buffer::from(value.to_vec()),
    }
}

#[test]
fn input_keeps_the_key_and_the_order_and_repeats_of_its_headers() {
    let block = RecordHeadersInput {
        key: Some(Buffer::from(b"k".to_vec())),
        headers: Some(vec![field("a", b"1"), field("b", b"2"), field("a", b"3")]),
    }
    .into_client();
    assert_eq!(block.key, Some(Bytes::from_static(b"k")));
    let names: Vec<_> = block
        .headers
        .iter()
        .map(|(n, v)| (n.as_str(), v.as_ref()))
        .collect();
    assert_eq!(
        names,
        [("a", &b"1"[..]), ("b", &b"2"[..]), ("a", &b"3"[..])]
    );
}

#[test]
fn input_with_nothing_is_an_empty_block() {
    let block = RecordHeadersInput {
        key: None,
        headers: None,
    }
    .into_client();
    assert!(block.is_empty());
}

#[test]
fn a_block_from_the_wire_keeps_its_key_and_headers() {
    let out = RecordHeaders::from(felix_wire::RecordHeaders {
        key: None,
        headers: vec![("trace".to_string(), Bytes::from_static(b"x"))],
    });
    assert!(matches!(out.key, napi::bindgen_prelude::Either::B(_)));
    assert_eq!(out.headers.len(), 1);
    assert_eq!(out.headers[0].name, "trace");
}
