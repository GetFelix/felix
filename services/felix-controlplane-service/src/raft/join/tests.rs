use super::*;

fn answers<const N: usize>(answers: [(NodeId, Answer); N]) -> BTreeMap<NodeId, Answer> {
    BTreeMap::from(answers)
}

fn down() -> Answer {
    Answer::Unreachable("connection refused".to_string())
}

fn refused() -> Answer {
    Answer::Unreadable("answered 403 Forbidden".to_string())
}

#[test]
fn every_member_empty_is_a_first_boot() {
    let all = answers([(2, Answer::Empty), (3, Answer::Empty)]);
    assert_eq!(decide(&all, &BTreeSet::new(), 3), Decision::Form);
    assert_eq!(decide(&answers([]), &BTreeSet::new(), 1), Decision::Form);
}

/// Two of three empty is a majority, and still not a first boot: the
/// silent member may hold the group.
#[test]
fn an_empty_majority_does_not_form_past_a_silent_member() {
    for third in [down(), refused()] {
        let some = answers([(2, Answer::Empty), (3, third)]);
        assert!(matches!(
            decide(&some, &BTreeSet::new(), 3),
            Decision::Wait(waiting) if waiting.len() == 1 && waiting[0].contains("member 3")
        ));
    }
}

#[test]
fn a_group_that_exists_is_never_formed_over() {
    let waived = BTreeSet::from([3]);
    for third in [Answer::Empty, down(), refused()] {
        let some = answers([(2, Answer::Exists), (3, third)]);
        assert_eq!(decide(&some, &BTreeSet::new(), 3), Decision::Exists);
        assert_eq!(decide(&some, &waived, 3), Decision::Exists);
    }
}

#[test]
fn a_waiver_excuses_only_silence() {
    let waived = BTreeSet::from([3]);
    let silent = answers([(2, Answer::Empty), (3, down())]);
    assert_eq!(decide(&silent, &waived, 3), Decision::Form);
    let answering = answers([(2, Answer::Empty), (3, Answer::Empty)]);
    assert_eq!(decide(&answering, &waived, 3), Decision::Form);
    let refusing = answers([(2, Answer::Empty), (3, refused())]);
    assert!(matches!(decide(&refusing, &waived, 3), Decision::Wait(_)));
    // The waiver names slot 3, not slot 2.
    let other = answers([(2, down()), (3, Answer::Empty)]);
    assert!(matches!(decide(&other, &waived, 3), Decision::Wait(_)));
}

/// The members that answer empty must be a majority on their own, so a
/// waiver can name at most a minority.
#[test]
fn a_waiver_cannot_name_a_majority() {
    let waived = BTreeSet::from([2, 3]);
    let alone = answers([(2, down()), (3, down())]);
    assert!(matches!(
        decide(&alone, &waived, 3),
        Decision::Wait(waiting) if waiting[0].contains("not a majority")
    ));
    let waived = BTreeSet::from([4, 5]);
    let three_of_five = answers([
        (2, Answer::Empty),
        (3, Answer::Empty),
        (4, down()),
        (5, down()),
    ]);
    assert_eq!(decide(&three_of_five, &waived, 5), Decision::Form);
    let waived = BTreeSet::from([3, 4, 5]);
    let two_of_five = answers([(2, Answer::Empty), (3, down()), (4, down()), (5, down())]);
    assert!(matches!(
        decide(&two_of_five, &waived, 5),
        Decision::Wait(_)
    ));
}
