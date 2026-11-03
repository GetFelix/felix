use std::sync::Arc;

use felix_storage::disk_log::{Member, Membership};
use tokio::sync::Notify;

use super::*;

fn key() -> ShardKey {
    ShardKey {
        tenant_id: "t".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: 0,
        kind: felix_router::ShardKind::Stream,
    }
}

fn set(generation: u64) -> Membership {
    Membership {
        generation,
        members: ["a", "b", "c"]
            .into_iter()
            .map(|node| Member {
                node: node.to_string(),
                storage: None,
                learner: false,
                wiped: false,
            })
            .collect(),
    }
}

fn named(generation: u64, incarnation: u64, leader: &str) -> Option<Named> {
    Some(Named {
        generation,
        incarnation,
        leader: leader.to_string(),
    })
}

fn elected(leader: &str, generation: u64) -> Elected {
    Elected {
        leader: leader.to_string(),
        generation,
        set: set(3),
        incarnation: 0,
    }
}

/// A generation has one leader: an equal or older record changes nothing.
#[test]
fn only_a_newer_generation_replaces_a_record() {
    let elections = Elections::new("a", Arc::new(Notify::new()));
    assert!(elections.record(key(), elected("a", 5)));
    assert!(!elections.record(key(), elected("b", 5)));
    assert!(!elections.record(key(), elected("b", 4)));
    assert!(elections.record(key(), elected("b", 6)));
    assert_eq!(elections.get(&key()).map(|e| e.leader), Some("b".into()));
}

/// The overlay drops a leadership once the control plane names its
/// generation or a newer one.
#[test]
fn the_overlay_drops_what_the_control_plane_caught_up_with() {
    let elections = Elections::new("a", Arc::new(Notify::new()));
    elections.record(key(), elected("a", 5));
    assert_eq!(elections.overlay(|_| named(4, 0, "z")).len(), 1);
    assert!(elections.overlay(|_| named(5, 0, "a")).is_empty());
    assert!(elections.get(&key()).is_none());
}

/// The control plane named another broker at this broker's own elected
/// generation. This broker may be the one that opened there, which the other
/// then never can, so its standing stays until a newer generation is named
/// and its reports can tell the control plane. A follower's record of
/// another's standing goes, as does one the control plane adopted.
#[test]
fn a_standing_the_control_plane_collided_with_stays_until_a_newer_generation() {
    let elections = Elections::new("a", Arc::new(Notify::new()));
    elections.record(key(), elected("a", 5));
    assert_eq!(elections.overlay(|_| named(5, 0, "b")).len(), 1);
    assert!(elections.reported(&key(), 5).is_some());
    assert!(elections.overlay(|_| named(6, 0, "b")).is_empty());

    let follower = Elections::new("c", Arc::new(Notify::new()));
    follower.record(key(), elected("a", 5));
    assert!(follower.overlay(|_| named(5, 0, "b")).is_empty());
}

/// The overlay drops a leadership whose shard has no route, since its stream
/// was deleted, or a route for another incarnation, since it was recreated.
#[test]
fn the_overlay_drops_a_deleted_or_recreated_streams_leadership() {
    let elections = Elections::new("a", Arc::new(Notify::new()));
    elections.record(key(), elected("a", 5));
    assert!(elections.overlay(|_| None).is_empty());
    assert!(elections.get(&key()).is_none());

    elections.record(key(), elected("a", 5));
    assert!(elections.overlay(|_| named(4, 1, "z")).is_empty());
    assert!(elections.get(&key()).is_none());
}

/// A report names the elected leadership only for this broker's own, at its
/// generation: the followers of the set it stood on, and that set's
/// generation, which the control plane compares with the set it records.
#[test]
fn a_report_names_only_this_brokers_own_leadership() {
    let elections = Elections::new("a", Arc::new(Notify::new()));
    elections.record(key(), elected("a", 5));
    assert_eq!(
        elections.reported(&key(), 5),
        Some(ElectedLeadership {
            leader: "a".to_string(),
            generation: 5,
            replicas: vec!["b".to_string(), "c".to_string()],
            set_generation: 3,
        })
    );
    assert_eq!(elections.reported(&key(), 4), None);
    elections.record(key(), elected("b", 6));
    assert_eq!(elections.reported(&key(), 6), None);
}

/// A fence in progress is marked until its guard drops, and only for its
/// generation.
#[test]
fn a_fence_in_progress_is_marked_for_its_generation() {
    let elections = Elections::new("a", Arc::new(Notify::new()));
    let fence = elections.fencing(&key(), 5);
    assert!(elections.is_fencing(&key(), 5));
    assert!(!elections.is_fencing(&key(), 6));
    drop(fence);
    assert!(!elections.is_fencing(&key(), 5));
}

/// Only this broker's own standing, at the route's generation, makes a
/// candidate's fence, and it carries the kept set's generation.
#[test]
fn a_candidacy_is_this_brokers_own_at_the_generation() {
    let elections = Elections::new("a", Arc::new(Notify::new()));
    elections.record(key(), elected("a", 5));
    let candidacy = elections.candidacy(&key(), 5).expect("standing");
    assert_eq!(candidacy.set.generation, 3);
    assert!(elections.candidacy(&key(), 6).is_none());
    elections.record(key(), elected("b", 6));
    assert!(elections.candidacy(&key(), 6).is_none());
}

/// Withdrawing names the generation, so a newer record survives a late
/// withdrawal of an older one.
#[test]
fn a_withdrawal_takes_only_its_own_generation() {
    let elections = Elections::new("a", Arc::new(Notify::new()));
    elections.record(key(), elected("a", 5));
    elections.record(key(), elected("b", 6));
    elections.withdraw(&key(), 5);
    assert_eq!(elections.get(&key()).map(|e| e.generation), Some(6));
    elections.withdraw(&key(), 6);
    assert!(elections.get(&key()).is_none());
}
