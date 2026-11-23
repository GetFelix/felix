use super::*;

const LEASE: Duration = Duration::from_millis(1500);
const ELECTION_MAX: Duration = Duration::from_secs(2);

#[test]
fn one_member_passing_the_lease_is_no_finding() {
    assert_eq!(two_serving(0, &[]), None);
    assert_eq!(two_serving(0, &[2]), None);
}

#[test]
fn two_members_passing_the_lease_at_once_is_a_finding() {
    let finding = two_serving(1, &[0, 2]).expect("two members passed");
    assert!(finding.contains("b1 and b3"), "{finding}");
}

#[test]
fn the_lease_is_judged_only_on_rates_it_was_sized_for() {
    assert!(lease_judged(&[1_000_000; 3], LEASE, ELECTION_MAX));
    // 1.3 apart fits 2 s / 1.5 s; 1.5 apart does not.
    assert!(lease_judged(&[900_000, 1_170_000], LEASE, ELECTION_MAX));
    assert!(!lease_judged(&[800_000, 1_200_000], LEASE, ELECTION_MAX));
    assert!(!lease_judged(&[], LEASE, ELECTION_MAX));
}

#[test]
fn copies_that_hold_one_offset_at_two_terms_disagree() {
    let logs = vec![
        (0, BTreeMap::from([(0, Some(1)), (1, Some(1))])),
        (1, BTreeMap::from([(0, Some(1)), (1, Some(2))])),
    ];
    let finding = disagreement(0, &logs).expect("offset 1 differs");
    assert!(finding.contains("offset 1"), "{finding}");
}

#[test]
fn entries_below_a_copys_base_and_offsets_one_copy_lacks_are_not_compared() {
    let logs = vec![
        (
            0,
            BTreeMap::from([(0, Some(1)), (1, Some(2)), (2, Some(2))]),
        ),
        (1, BTreeMap::from([(0, None), (1, Some(2))])),
    ];
    assert_eq!(disagreement(0, &logs), None);
}

#[test]
fn a_shard_burns_terms_only_past_what_its_faults_explain() {
    let spans = BTreeMap::from([
        (
            0,
            Span {
                first: 1,
                last: 1 + 2 * 3 + 2,
            },
        ),
        (
            1,
            Span {
                first: 1,
                last: 1 + 2 * 3 + 3,
            },
        ),
    ]);
    let burned = burned(&spans, 3);
    assert_eq!(burned.len(), 1, "{burned:?}");
    assert!(burned[0].contains("/s1"), "{burned:?}");
}

#[test]
fn a_span_keeps_the_lowest_and_highest_term() {
    let mut spans = BTreeMap::new();
    for term in [4, 2, 7, 3] {
        Span::saw(&mut spans, 0, term);
    }
    let span = spans[&0];
    assert_eq!((span.first, span.last), (2, 7));
}
