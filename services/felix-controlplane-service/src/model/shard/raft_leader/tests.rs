//! Which floor report names the broker a Raft group elected.
use super::*;
use crate::model::{ShardKey, ShardKind, ShardState};

fn assignment() -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "payments".to_string(),
            stream: "orders".to_string(),
            shard: 2,
            kind: ShardKind::Stream,
        },
        leader: "broker-a".to_string(),
        replicas: vec!["broker-b".to_string(), "broker-c".to_string()],
        generation: 7,
        state: ShardState::Active,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
        incarnation: 3,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        learners: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
        placed: None,
        switch: None,
        raft_copy_ids: Default::default(),
        raft_target_counter: 0,
        raft_target: None,
        raft_floor: None,
        raft_formed: None,
        raft_voters: None,
        raft_seed: None,
        raft_bases: None,
        raft_leader: None,
        raft_ordered: None,
    }
}

fn report(a: &ShardAssignment, term: u64) -> RaftFloorReport {
    RaftFloorReport {
        key: a.key.clone(),
        incarnation: a.incarnation,
        epoch: 0,
        index: term * 10,
        term,
        voters: None,
    }
}

#[test]
fn the_highest_term_names_the_leader() {
    let mut a = assignment();
    assert!(a.note_raft_leader("broker-b", &report(&a, 4)));
    assert_eq!(a.reported_leader(), Some("broker-b"));
    // A deposed leader that has not noticed reports an older term.
    assert!(!a.note_raft_leader("broker-c", &report(&a, 3)));
    assert_eq!(a.reported_leader(), Some("broker-b"));
    assert!(a.note_raft_leader("broker-c", &report(&a, 5)));
    assert_eq!(a.reported_leader(), Some("broker-c"));
}

#[test]
fn the_first_reporter_of_a_term_keeps_it() {
    let mut a = assignment();
    assert!(a.note_raft_leader("broker-b", &report(&a, 4)));
    // Two reports at one term: the second is never adopted, so repeated
    // reports cannot flip the name back and forth.
    for _ in 0..3 {
        assert!(!a.note_raft_leader("broker-c", &report(&a, 4)));
        assert!(!a.note_raft_leader("broker-b", &report(&a, 4)));
    }
    assert_eq!(a.reported_leader(), Some("broker-b"));
}

#[test]
fn a_reporter_outside_the_set_or_its_voters_is_ignored() {
    let mut a = assignment();
    assert!(!a.note_raft_leader("broker-x", &report(&a, 9)));
    let mut without_self = report(&a, 9);
    without_self.voters = Some(
        [("broker-a", 1), ("broker-c", 1)]
            .into_iter()
            .map(|(node, copy)| (node.to_string(), copy))
            .collect(),
    );
    assert!(!a.note_raft_leader("broker-b", &without_self));
    assert_eq!(a.raft_leader, None);
}

#[test]
fn another_group_is_ignored_and_a_new_incarnation_drops_it() {
    let mut a = assignment();
    let mut old = report(&a, 9);
    old.incarnation = 2;
    assert!(!a.note_raft_leader("broker-b", &old));
    let mut reformed = report(&a, 9);
    reformed.epoch = 1;
    assert!(!a.note_raft_leader("broker-b", &reformed));

    assert!(a.note_raft_leader("broker-b", &report(&a, 4)));
    let mut same = assignment();
    same.carry_raft_leader(Some(&a));
    assert_eq!(same.reported_leader(), Some("broker-b"));
    let mut next = assignment();
    next.incarnation = 4;
    next.carry_raft_leader(Some(&a));
    assert_eq!(next.raft_leader, None);
}
