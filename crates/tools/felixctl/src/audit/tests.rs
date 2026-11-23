use serde_json::json;

use super::*;

#[test]
fn times_are_shown_in_utc() {
    assert_eq!(utc(0), "1970-01-01 00:00:00");
    assert_eq!(utc(1_795_369_947_596), "2026-11-22 17:52:27");
    assert_eq!(utc(951_782_400_000), "2000-02-29 00:00:00");
}

#[test]
fn records_render_as_a_table() {
    let records = [json!({
        "seq": 3, "at_ms": 0, "member": "1", "method": "POST",
        "path": "/v1/tenants/t1", "status": 403, "subject": "alice",
        "credential": "felix_token", "refusal": "forbidden",
    })];
    let text = render(&records);
    let mut lines = text.lines();
    assert!(lines.next().expect("header").starts_with("SEQ"));
    let row = lines.next().expect("row");
    for field in [
        "3",
        "1970-01-01 00:00:00",
        "POST",
        "/v1/tenants/t1",
        "403",
        "alice",
        "forbidden",
    ] {
        assert!(row.contains(field), "{field} missing from {row}");
    }
}

#[test]
fn a_reader_is_told_when_records_were_trimmed_first() {
    let page = json!({"records": [], "oldest_seq": 11, "last_seq": 20});
    assert_eq!(
        trimmed(&page, 4).as_deref(),
        Some("6 records after 4 were trimmed before they were read; the oldest kept is 11")
    );
    assert_eq!(trimmed(&page, 10), None);
    assert_eq!(trimmed(&json!({"records": [], "last_seq": 0}), 0), None);
}
