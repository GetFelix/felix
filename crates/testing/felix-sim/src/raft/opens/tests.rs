use super::*;

fn open(member: usize, term: u64) -> Open {
    Open {
        member,
        shard: 0,
        term,
    }
}

fn judged(opens: &[Open], led: &[(u64, usize)], committed: &[u64]) -> Vec<String> {
    let led = led
        .iter()
        .map(|&(term, member)| ((0, term), member))
        .collect();
    let committed = BTreeMap::from([(0, committed.iter().copied().collect())]);
    let seen = Seen {
        led: &led,
        committed: &committed,
        read: &BTreeMap::new(),
        before_read: &BTreeMap::new(),
    };
    judge(opens, &seen)
}

#[test]
fn each_term_opened_by_its_leader_with_an_entry_committed_passes() {
    let opens = [open(0, 1), open(1, 2), open(1, 2), open(2, 4)];
    // Term 3 elected b1, which never opened it: that is fine.
    let led = [(1, 0), (2, 1), (3, 0)];
    assert!(judged(&opens, &led, &[1, 2, 3, 4]).is_empty());
}

#[test]
fn two_brokers_opening_one_term_is_a_finding() {
    let findings = judged(&[open(0, 2), open(1, 2)], &[], &[2]);
    assert_eq!(findings, ["t/ns/s0: b1 and b2 both opened term 2"]);
}

#[test]
fn opening_a_term_raft_elected_another_broker_in_is_a_finding() {
    let findings = judged(&[open(1, 2)], &[(2, 0)], &[2]);
    assert_eq!(
        findings,
        ["t/ns/s0: b2 opened term 2, which Raft elected b1 in"]
    );
}

/// b1 was elected in term 2 and opened before its blank entry committed;
/// term 3's leader then cut the blank off. Nothing of term 2 is left.
#[test]
fn opening_a_term_whose_blank_never_committed_is_a_finding() {
    let findings = judged(&[open(0, 2)], &[(2, 0)], &[1, 3]);
    assert_eq!(findings.len(), 1);
    assert!(findings[0].contains("blank entry was never committed"));
}

/// The final log is held to the opens before it was read: one before it
/// with no entry in it is a finding, and one after it is judged on the other
/// rules only, since its entry cannot be in a log read before it (#3126).
#[test]
fn only_opens_before_the_final_read_are_judged_against_it() {
    let committed = BTreeMap::from([(0, BTreeSet::from([1, 5]))]);
    let judged = |opens: &[Open], before: usize| {
        let seen = Seen {
            led: &BTreeMap::new(),
            committed: &committed,
            read: &BTreeMap::new(),
            before_read: &BTreeMap::from([(0, before)]),
        };
        judge(opens, &seen)
    };
    let before = judged(&[open(0, 5), open(1, 6)], 2);
    assert_eq!(before.len(), 1, "{before:?}");
    assert!(
        before[0].starts_with("t/ns/s0: b2 opened term 6, and the final log holds no entry"),
        "{before:?}"
    );
    assert!(judged(&[open(0, 5), open(1, 6)], 1).is_empty());
    let twice = judged(&[open(0, 5), open(1, 6), open(2, 6)], 1);
    assert_eq!(twice, ["t/ns/s0: b2 and b3 both opened term 6"]);
}

#[test]
fn a_shard_without_a_final_log_is_not_judged_on_it() {
    let seen = Seen {
        led: &BTreeMap::new(),
        committed: &BTreeMap::new(),
        read: &BTreeMap::new(),
        before_read: &BTreeMap::new(),
    };
    assert!(judge(&[open(0, 2)], &seen).is_empty());
}

/// The finding says where the log was read and who still holds the term,
/// which is what tells a log read from a stale leader from a blank entry
/// that is gone from every disk (#3126).
#[test]
fn a_missing_term_names_where_the_log_was_read_and_who_holds_the_term() {
    let committed = BTreeMap::from([(0, BTreeSet::from([1, 5]))]);
    let judged = |holders: &[usize]| {
        let read = Read {
            from: 1,
            term: 5,
            holders: BTreeMap::from([(6, holders.to_vec())]),
            logs: vec![
                MemberLog {
                    member: 0,
                    last: Some((5, 40)),
                    leader: false,
                },
                MemberLog {
                    member: 1,
                    last: Some((5, 41)),
                    leader: true,
                },
            ],
        };
        let seen = Seen {
            led: &BTreeMap::new(),
            committed: &committed,
            read: &BTreeMap::from([(0, read)]),
            before_read: &BTreeMap::new(),
        };
        judge(&[open(0, 6)], &seen).remove(0)
    };
    let held = judged(&[0, 3]);
    assert!(
        held.contains(
            "the log was read from b2 leading term 5, and b1 b4 still hold an entry of it; \
             judged against b1 term 5 last index 40; b2 term 5 last index 41, leader"
        ),
        "{held}"
    );
    let gone = judged(&[]);
    assert!(
        gone.contains("and no member's log holds an entry of it; judged against"),
        "{gone}"
    );
}
