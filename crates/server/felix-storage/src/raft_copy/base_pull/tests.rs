use super::*;

fn base(index: u64, term: u64) -> EpochBase {
    EpochBase {
        base_last: Some(EpochLogId {
            index,
            epoch: 1,
            term,
        }),
        first_index: index + 10,
    }
}

#[test]
fn a_whole_base_in_order_is_complete() {
    let mut check = BaseCheck::new(&base(5, 3), 2).expect("something to pull");
    for (index, term) in [(2, 1), (3, 1), (4, 2), (5, 3)] {
        assert!(!check.complete());
        check.take(index, term).expect("in order");
    }
    assert!(check.complete());
}

#[test]
fn a_hole_or_a_repeat_is_refused() {
    let mut check = BaseCheck::new(&base(5, 1), 2).expect("something to pull");
    check.take(2, 1).expect("first");
    assert_eq!(check.take(4, 1), Err(Mismatch::Index { wanted: 3, got: 4 }));
    assert_eq!(check.take(2, 1), Err(Mismatch::Index { wanted: 3, got: 2 }));
    assert!(!check.complete());
}

#[test]
fn an_entry_in_the_gap_is_refused() {
    let mut check = BaseCheck::new(&base(3, 1), 3).expect("something to pull");
    check.take(3, 1).expect("the base's last");
    assert_eq!(check.take(4, 1), Err(Mismatch::PastBase { index: 4 }));
}

#[test]
fn another_history_is_refused() {
    let mut check = BaseCheck::new(&base(4, 3), 1).expect("something to pull");
    check.take(1, 2).expect("first");
    assert_eq!(
        check.take(2, 1),
        Err(Mismatch::TermFalls {
            index: 2,
            term: 1,
            before: 2
        })
    );
    assert_eq!(
        check.take(2, 4),
        Err(Mismatch::TermPastBase { index: 2, term: 4 })
    );
    check.take(2, 2).expect("same term");
    check.take(3, 2).expect("same term");
    // A log that ends at the base's index in another term is not the base.
    assert_eq!(
        check.take(4, 2),
        Err(Mismatch::LastTerm { term: 2, wanted: 3 })
    );
    assert!(!check.complete());
}

#[test]
fn a_floor_raised_under_the_pull_restarts_it_there() {
    let mut check = BaseCheck::new(&base(9, 1), 0).expect("something to pull");
    check.take(0, 1).expect("first");
    check.take(1, 1).expect("second");
    assert!(!check.raise_floor(2), "the next entry is at the floor");
    assert!(check.raise_floor(5), "what is held is below the floor");
    assert_eq!(check.next(), 5);
    assert_eq!(check.take(2, 1), Err(Mismatch::Index { wanted: 5, got: 2 }));
    for index in 5..=9 {
        check.take(index, 1).expect("from the new floor");
    }
    assert!(check.complete());
}

#[test]
fn nothing_to_pull_for_an_empty_base_or_a_floor_past_it() {
    let empty = EpochBase {
        base_last: None,
        first_index: 4,
    };
    assert_eq!(BaseCheck::new(&empty, 0), None);
    assert_eq!(BaseCheck::new(&base(3, 1), 4), None);
}
