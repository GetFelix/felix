//! Abandoning a lost Raft group's log.
use std::collections::{BTreeMap, BTreeSet};

use felix_storage::raft_copy::{DEFAULT_OFFSET_GAP, RESTORE_OFFSETS_PER_MILLI};

use super::*;
use crate::cluster::placement::assignment_for;
use crate::model::{RaftFloor, RaftTarget, ShardKey, ShardKind};

fn set(nodes: &[&str]) -> BTreeSet<String> {
    nodes.iter().map(|n| n.to_string()).collect()
}

/// An RF 1 Raft shard on `a` at incarnation 3, epoch 0, generation 5, whose
/// leader last reported a commit at 40.
fn stranded() -> ShardAssignment {
    let key = ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "payments".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    };
    let mut shard = assignment_for(&key, "a", Vec::new());
    shard.generation = 5;
    shard.state = ShardState::Active;
    shard.incarnation = 3;
    shard.placed = Some(PlacedInEpoch {
        epoch: 0,
        brokers: set(&["a"]),
    });
    shard.raft_copy_ids = BTreeMap::from([("a".to_string(), 11)]);
    shard.raft_target = Some(RaftTarget {
        counter: 1,
        members: BTreeMap::from([("a".to_string(), 11)]),
    });
    shard.raft_floor = Some(RaftFloor {
        incarnation: 3,
        offset_floor: 40,
        epoch: 0,
        index: 40,
        term: 2,
    });
    shard.start_raft_ordered();
    shard
}

fn start(shard: &ShardAssignment, dead: &[&str]) -> AbandonEpochStart {
    AbandonEpochStart {
        incarnation: shard.incarnation,
        expected_epoch: shard.group_epoch(),
        expected_generation: shard.generation,
        leader: "b".to_string(),
        dead: set(dead),
        planned_at_millis: 1_800_000_000_000,
        offset_gap: DEFAULT_OFFSET_GAP,
    }
}

#[test]
fn an_abandon_is_refused_while_a_placed_broker_is_not_declared_dead() {
    let shard = stranded();
    assert_eq!(
        shard.abandon_epoch(&start(&shard, &[])),
        Err(AbandonError::Unaccounted("a".to_string()))
    );
    // A broker placed earlier in the epoch counts too: it may hold a copy.
    let mut moved = shard.clone();
    moved
        .placed
        .as_mut()
        .unwrap()
        .brokers
        .insert("c".to_string());
    assert_eq!(
        moved.abandon_epoch(&start(&moved, &["a"])),
        Err(AbandonError::Unaccounted("c".to_string()))
    );
    // So does one named by the assignment but not yet on the placed record.
    let mut replicated = shard.clone();
    replicated.replicas = vec!["d".to_string()];
    assert_eq!(
        replicated.abandon_epoch(&start(&replicated, &["a"])),
        Err(AbandonError::Unaccounted("d".to_string()))
    );
}

#[test]
fn an_abandon_starts_the_next_epoch_empty_on_the_new_leader() {
    let shard = stranded();
    let next = shard
        .abandon_epoch(&start(&shard, &["a"]))
        .expect("abandon");
    assert_eq!(next.leader, "b");
    assert!(next.replicas.is_empty());
    assert_eq!(next.generation, 6);
    assert_eq!(next.group_epoch(), 1);
    assert_eq!(next.placed.as_ref().unwrap().brokers, set(&["b"]));
    assert!(next.raft_copy_ids.is_empty(), "no old copy is a voter");
    assert!(next.raft_target.is_none());
    let bases = &next.raft_bases.as_ref().unwrap().bases;
    assert_eq!(bases.len(), 1);
    assert_eq!(bases[0].base_last, None);
    // The offset floor stays: offsets the lost group gave out stay given out.
    assert_eq!(next.raft_floor, shard.raft_floor);
    assert!(
        next.ordered_voter_sets()
            .is_some_and(|sets| sets.is_empty())
    );
}

/// The floor lags the lost leader by up to a second, and by much more when
/// it could not reach the control plane, so the first index is never just
/// the floor plus one: it is on the time line, above any offset the lost
/// group handed out since it last reported.
#[test]
fn offsets_after_an_abandon_are_above_every_offset_the_lost_group_gave_out() {
    let shard = stranded();
    let start = start(&shard, &["a"]);
    let next = shard.abandon_epoch(&start).expect("abandon");
    let first = next.raft_bases.as_ref().unwrap().bases[0].first_index;
    assert!(first > 40, "above the floor");
    // The lost leader writing at half the line's slope since the epoch
    // began at Unix time 0, which no real shard comes near, plus a gap's
    // worth of unreported acks, is still below the first index.
    let unreported = RESTORE_OFFSETS_PER_MILLI / 2 * start.planned_at_millis;
    assert!(first > unreported + 40);
    assert_eq!(
        first,
        RESTORE_OFFSETS_PER_MILLI * start.planned_at_millis + DEFAULT_OFFSET_GAP
    );

    // An earlier epoch's first index above the line still bounds it.
    let mut later = next.clone();
    later.placed.as_mut().unwrap().brokers = set(&["b"]);
    let mut again = AbandonEpochStart {
        expected_epoch: 1,
        expected_generation: later.generation,
        leader: "c".to_string(),
        dead: set(&["b"]),
        planned_at_millis: 0,
        ..start.clone()
    };
    again.offset_gap = DEFAULT_OFFSET_GAP;
    let third = later.abandon_epoch(&again).expect("second abandon");
    let bases = &third.raft_bases.as_ref().unwrap().bases;
    assert_eq!(bases.len(), 2);
    assert_eq!(bases[1].first_index, first + 1);
}

#[test]
fn an_abandon_with_a_short_gap_is_refused() {
    let shard = stranded();
    let short = AbandonEpochStart {
        offset_gap: MIN_OFFSET_GAP - 1,
        ..start(&shard, &["a"])
    };
    assert_eq!(
        shard.abandon_epoch(&short),
        Err(AbandonError::ShortGap(MIN_OFFSET_GAP - 1))
    );
}

/// A replayed abandon finds the shard one epoch and one generation on, so
/// it is refused; `abandoned_by` tells the caller it already landed.
#[test]
fn a_replayed_abandon_never_starts_a_second_epoch() {
    let shard = stranded();
    let start = start(&shard, &["a"]);
    let next = shard.abandon_epoch(&start).expect("abandon");
    assert!(next.abandoned_by(&start));
    assert!(!shard.abandoned_by(&start));
    assert!(matches!(
        next.abandon_epoch(&start),
        Err(AbandonError::EpochMoved { .. })
    ));
}

#[test]
fn an_abandon_asked_at_another_generation_or_incarnation_is_refused() {
    let shard = stranded();
    let stale = AbandonEpochStart {
        expected_generation: 4,
        ..start(&shard, &["a"])
    };
    assert!(matches!(
        shard.abandon_epoch(&stale),
        Err(AbandonError::GenerationMoved { .. })
    ));
    let other = AbandonEpochStart {
        incarnation: 2,
        ..start(&shard, &["a"])
    };
    assert!(matches!(
        shard.abandon_epoch(&other),
        Err(AbandonError::OtherIncarnation { .. })
    ));
    let onto_dead = AbandonEpochStart {
        leader: "a".to_string(),
        ..start(&shard, &["a"])
    };
    assert_eq!(
        shard.abandon_epoch(&onto_dead),
        Err(AbandonError::LeaderDead("a".to_string()))
    );
}
