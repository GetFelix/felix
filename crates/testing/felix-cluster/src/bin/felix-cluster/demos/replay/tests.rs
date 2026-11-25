use super::*;

fn log(records: &[(u64, &str)]) -> Vec<Record> {
    records
        .iter()
        .map(|(offset, payload)| Record::new(Some(*offset), *payload))
        .collect()
}

/// The #3112 shape: only the harness's probe comes back.
#[test]
fn a_replay_that_ends_below_the_last_record_is_short() {
    let replayed = log(&[(0, "harness-probe")]);
    let last = Record::new(Some(5), "after-2");
    assert_eq!(
        reach(&replayed, &last).unwrap(),
        Reach::Short {
            stopped_at: Some(0)
        }
    );
    assert_eq!(
        reach(&[], &last).unwrap(),
        Reach::Short { stopped_at: None }
    );
}

#[test]
fn a_replay_through_the_last_record_is_whole() {
    // Offset 3 holds no record, as after an election on Raft.
    let replayed = log(&[(0, "harness-probe"), (1, "before-1"), (4, "after-2")]);
    let last = Record::new(Some(4), "after-2");
    assert_eq!(reach(&replayed, &last).unwrap(), Reach::Whole);
}

/// A resend left an earlier copy; the acknowledged one is further on.
#[test]
fn an_earlier_copy_of_the_last_record_does_not_make_a_replay_whole() {
    let replayed = log(&[(0, "before-1"), (1, "after-2")]);
    let last = Record::new(Some(2), "after-2");
    assert_eq!(
        reach(&replayed, &last).unwrap(),
        Reach::Short {
            stopped_at: Some(1)
        }
    );
}

#[test]
fn offsets_that_do_not_climb_fail_the_replay() {
    let replayed = log(&[(0, "a"), (2, "b"), (2, "c")]);
    let err = reach(&replayed, &Record::new(Some(2), "c")).unwrap_err();
    assert!(err.to_string().contains("offset 2 after offset 2"), "{err}");
}

#[test]
fn an_acknowledged_record_a_whole_replay_read_past_is_missing() {
    let acknowledged = log(&[(1, "before-1"), (2, "before-2"), (3, "before-3")]);
    let replayed = log(&[(0, "harness-probe"), (1, "before-1"), (3, "before-3")]);
    assert_eq!(missing(&acknowledged, &replayed), log(&[(2, "before-2")]));
    // Another record at the acknowledged offset is not that record.
    let overwritten = log(&[(1, "before-1"), (2, "marker"), (3, "before-3")]);
    assert_eq!(
        missing(&acknowledged, &overwritten),
        log(&[(2, "before-2")])
    );
    assert!(missing(&acknowledged, &acknowledged).is_empty());
}

#[test]
fn a_record_with_no_offset_is_matched_by_its_payload() {
    let acknowledged = vec![Record::new(None, "before-1")];
    assert!(missing(&acknowledged, &log(&[(7, "before-1")])).is_empty());
}
