//! When a Raft shard's target counter rises.
use std::collections::BTreeMap;

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
        incarnation: 0,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        learners: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
        placed: None,
        switch: None,
        raft_copy_ids: Default::default(),
        raft_target: None,
    }
}

fn ids(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
    pairs.iter().map(|(n, id)| (n.to_string(), *id)).collect()
}

fn reported(mut a: ShardAssignment, pairs: &[(&str, u64)]) -> ShardAssignment {
    a.raft_copy_ids.extend(ids(pairs));
    a
}

#[test]
fn no_target_until_every_broker_in_the_set_reported() {
    let mut a = reported(assignment(), &[("broker-a", 1), ("broker-b", 2)]);
    assert_eq!(a.settle_raft_target(), Ok(false));
    assert_eq!(a.raft_target, None);
    a.raft_copy_ids.insert("broker-c".into(), 3);
    assert_eq!(a.settle_raft_target(), Ok(true));
    let target = a.raft_target.clone().expect("target");
    assert_eq!(target.counter, 1);
    assert_eq!(
        target.members,
        ids(&[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)])
    );
}

#[test]
fn the_same_set_keeps_the_counter() {
    let mut a = reported(
        assignment(),
        &[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)],
    );
    a.settle_raft_target().expect("first");
    // A write naming the same set, its leader swapped among the members.
    a.leader = "broker-b".into();
    a.replicas = vec!["broker-a".into(), "broker-c".into()];
    assert_eq!(a.settle_raft_target(), Ok(false));
    assert_eq!(a.raft_target.as_ref().map(|t| t.counter), Some(1));
}

#[test]
fn a_set_change_takes_the_next_counter_and_drops_the_leavers_id() {
    let mut a = reported(
        assignment(),
        &[
            ("broker-a", 1),
            ("broker-b", 2),
            ("broker-c", 3),
            ("broker-d", 4),
        ],
    );
    a.settle_raft_target().expect("first");
    assert!(!a.raft_copy_ids.contains_key("broker-d"));
    a.replicas = vec!["broker-b".into(), "broker-d".into()];
    a.raft_copy_ids.insert("broker-d".into(), 4);
    assert_eq!(a.settle_raft_target(), Ok(true));
    let target = a.raft_target.expect("target");
    assert_eq!(target.counter, 2);
    assert_eq!(
        target.members,
        ids(&[("broker-a", 1), ("broker-b", 2), ("broker-d", 4)])
    );
    assert!(!a.raft_copy_ids.contains_key("broker-c"));
}

#[test]
fn a_new_copy_id_takes_the_next_counter() {
    let mut a = reported(
        assignment(),
        &[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)],
    );
    a.settle_raft_target().expect("first");
    a.raft_copy_ids.insert("broker-b".into(), 9);
    assert_eq!(a.settle_raft_target(), Ok(true));
    assert_eq!(a.raft_target.as_ref().map(|t| t.counter), Some(2));
    // The same id again changes nothing.
    a.raft_copy_ids.insert("broker-b".into(), 9);
    assert_eq!(a.settle_raft_target(), Ok(false));
    assert_eq!(a.raft_target.as_ref().map(|t| t.counter), Some(2));
}

#[test]
fn the_counter_is_never_reused_at_its_limit() {
    let mut a = reported(
        assignment(),
        &[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)],
    );
    a.raft_target = Some(RaftTarget {
        counter: u64::MAX,
        members: ids(&[("broker-a", 1)]),
    });
    assert_eq!(
        a.settle_raft_target(),
        Err(RaftTargetError::CounterExhausted)
    );
}
