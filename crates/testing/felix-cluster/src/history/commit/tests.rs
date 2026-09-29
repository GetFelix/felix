use std::collections::BTreeSet;

use super::{CommitRead, StateSeen, check};
use crate::history::Rule;
use crate::history::model::Element;

fn elements(pairs: &[(u64, u64)]) -> Vec<Element> {
    pairs
        .iter()
        .map(|&(offset, value)| Element {
            offset,
            value,
            skipped_before: 0,
        })
        .collect()
}

fn read(before: &[(u64, u64)], state: Option<(u64, u64)>, after: &[(u64, u64)]) -> CommitRead {
    CommitRead {
        process: 0,
        invoke: 0,
        complete: 1,
        list: "a".to_string(),
        before: elements(before),
        state: state.map(|(value, version)| StateSeen { value, version }),
        after: state.map(|_| elements(after)),
    }
}

fn rules(reads: &[CommitRead]) -> Vec<Rule> {
    // Values 10 and 11 are commits; 1 is a plain append.
    let commits: BTreeSet<u64> = [10, 11].into();
    let mut out = Vec::new();
    check(reads, &commits, &mut out);
    out.into_iter().map(|violation| violation.rule).collect()
}

#[test]
fn a_reader_that_sees_both_halves_is_valid() {
    assert!(rules(&[read(&[(0, 1), (1, 10)], Some((10, 1)), &[(1, 10), (2, 11)])]).is_empty());
    // The state may be newer than the first read: a commit landed between.
    assert!(rules(&[read(&[(1, 10)], Some((11, 2)), &[(2, 11)])]).is_empty());
    // Nothing committed yet, and nothing seen.
    assert!(rules(&[read(&[(0, 1)], None, &[])]).is_empty());
}

#[test]
fn the_event_without_the_state_is_a_partial_commit() {
    assert_eq!(
        rules(&[read(&[(1, 10), (2, 11)], Some((10, 1)), &[(1, 10)])]),
        [Rule::PartialCommit]
    );
    assert_eq!(rules(&[read(&[(1, 10)], None, &[])]), [Rule::PartialCommit]);
}

#[test]
fn the_state_without_the_event_is_a_partial_commit() {
    assert_eq!(
        rules(&[read(&[], Some((10, 1)), &[(2, 11)])]),
        [Rule::PartialCommit]
    );
    assert_eq!(
        rules(&[read(&[], Some((10, 1)), &[(1, 11)])]),
        [Rule::PartialCommit]
    );
    assert_eq!(
        rules(&[read(&[], Some((1, 0)), &[(0, 1)])]),
        [Rule::PartialCommit]
    );
}
