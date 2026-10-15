use serde_json::json;

use super::*;

#[test]
fn a_table_pads_every_column_but_the_last() {
    let text = table(
        &["NAME", "SHARDS", "STATE"],
        vec![
            vec!["orders".into(), "4".into(), "active".into()],
            vec!["x".into(), "12".into(), "moving".into()],
        ],
    );
    assert_eq!(
        text,
        "NAME    SHARDS  STATE\n\
         orders  4       active\n\
         x       12      moving"
    );
}

#[test]
fn a_table_with_no_rows_is_its_header() {
    assert_eq!(table(&["A", "B"], Vec::new()), "A  B");
}

#[test]
fn text_payloads_stay_text_and_binary_ones_become_base64() {
    assert_eq!(payload_field(b"hello"), ("payload", json!("hello")));
    assert_eq!(
        payload_field(&[0xff, 0x00]),
        ("payload_base64", json!("/wA="))
    );
}

#[test]
fn cells_render_plainly() {
    assert_eq!(cell(&json!(null)), "");
    assert_eq!(cell(&json!("a")), "a");
    assert_eq!(cell(&json!(["a", "b"])), "a,b");
    assert_eq!(cell(&json!(3)), "3");
    assert_eq!(cell(&json!({"k": 1})), r#"{"k":1}"#);
}

#[test]
fn fields_line_up_keys() {
    let text = fields(&json!({"stream": "orders", "shards": 4}));
    let mut lines: Vec<&str> = text.lines().collect();
    lines.sort();
    assert_eq!(lines, ["shards  4", "stream  orders"]);
}

#[test]
fn text_reaches_a_terminal_unchanged() {
    assert!(matches!(
        for_terminal("héllo\tworld".as_bytes()),
        Cow::Borrowed(_)
    ));
}

#[test]
fn binary_is_escaped_for_a_terminal() {
    let bytes = [b'A', 0x00, 0x1b, b'[', 0xff, b'z', 0xc2, 0x85];
    assert_eq!(
        String::from_utf8(for_terminal(&bytes).into_owned()).unwrap(),
        "A\\x00\\x1b[\\xffz\\u{85}"
    );
}
