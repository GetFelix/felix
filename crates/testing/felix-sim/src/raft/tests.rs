use super::*;

fn entry(term: u64, value: u64) -> Held {
    Held {
        generation: Some(term),
        mark: RecordMark::None,
        payload: Bytes::from(value.to_le_bytes().to_vec()),
    }
}

fn flushed(term: u64, index: u64) -> Option<LogPoint> {
    Some(LogPoint { term, index })
}

/// b1 led term 1 and, cut off, flushed its own tail to 120. b2 leads term 2
/// and acknowledges offset 100 with only itself flushed. b1's index is past
/// 100, but its record there is not b2's, so only one member holds it.
#[test]
fn a_deposed_leaders_flushed_tail_does_not_hold_the_new_record() {
    let acknowledged = entry(2, 7);
    let members = [
        (flushed(1, 120), Some(entry(1, 3))),
        (flushed(2, 100), Some(acknowledged.clone())),
        (None, None),
    ];
    assert_eq!(holders(&acknowledged, 100, &members), 1);
}

#[test]
fn a_member_holds_a_record_once_it_has_the_same_one_flushed() {
    let record = entry(2, 7);
    let members = [
        (flushed(2, 100), Some(record.clone())),
        (flushed(2, 104), Some(record.clone())),
        // Written but not yet flushed.
        (flushed(2, 99), Some(record.clone())),
    ];
    assert_eq!(holders(&record, 100, &members), 2);
}
