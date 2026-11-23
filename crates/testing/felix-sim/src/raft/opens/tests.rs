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

#[test]
fn a_shard_without_a_final_log_is_not_judged_on_it() {
    let seen = Seen {
        led: &BTreeMap::new(),
        committed: &BTreeMap::new(),
    };
    assert!(judge(&[open(0, 2)], &seen).is_empty());
}
