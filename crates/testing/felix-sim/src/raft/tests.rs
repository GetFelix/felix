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

/// A wiped copy held offset 50 as term 1's value 3. If two members later
/// disagree at 50, the credit backs only the record the copy held.
#[test]
fn a_replaced_copy_is_credited_only_with_the_record_it_held() {
    let held = entry(1, 3);
    let credit = Credit {
        bound: 100,
        records: BTreeMap::from([(50, held.clone())]),
    };
    assert!(credited(Some(&credit), 50, &held));
    assert!(!credited(Some(&credit), 50, &entry(2, 9)));
    assert!(!credited(Some(&credit), 60, &held));
    assert!(!credited(None, 50, &held));
}

/// What a stopping copy of five is credited with, and whether that makes
/// `acked` at offset 100 look held by a majority. b4 and b5 flushed it; b1
/// and b2 are behind; b3 stopped with 100 in its log.
fn held_with_stopped_b3(reported: Option<LogPoint>, durable: u64, acked: &Held) -> usize {
    let log: BTreeMap<u64, Held> = (95..=100)
        .map(|offset| {
            (
                offset,
                if offset == 100 {
                    acked.clone()
                } else {
                    entry(4, offset)
                },
            )
        })
        .collect();
    let bound = flushed_bound(reported, durable);
    let credit = Credit {
        bound,
        records: log.range(..bound).map(|(o, h)| (*o, h.clone())).collect(),
    };
    let b3 = if credited(Some(&credit), 100, acked) {
        (flushed(0, 100), Some(acked.clone()))
    } else {
        (None, None)
    };
    let members = [
        (flushed(4, 99), None),
        (flushed(4, 99), None),
        b3,
        (flushed(4, 100), Some(acked.clone())),
        (flushed(4, 100), Some(acked.clone())),
    ];
    holders(acked, 100, &members)
}

/// A follower flushes and acks before it learns the commit, so a copy
/// stopped right after counts for 100 even with its read bound at 100.
#[test]
fn a_stopped_copy_is_credited_with_what_it_flushed() {
    let acked = entry(4, 7);
    assert_eq!(held_with_stopped_b3(flushed(4, 100), 101, &acked), 3);
}

/// Only two members ever flushed 100. b3 had it in its log but not flushed
/// (reported 99), or had it rewritten after reporting (durable bound 100):
/// either way the credit must leave the ack under-replicated.
#[test]
fn a_stopped_copy_is_not_credited_past_what_it_flushed() {
    let acked = entry(4, 7);
    assert_eq!(held_with_stopped_b3(flushed(4, 99), 101, &acked), 2);
    assert_eq!(held_with_stopped_b3(flushed(4, 100), 100, &acked), 2);
    assert_eq!(held_with_stopped_b3(None, 101, &acked), 2);
}
