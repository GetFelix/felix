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
        kind: crate::ShardKind::Stream,
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

fn elected(leader: &str, generation: u64) -> Elected {
    Elected {
        leader: leader.to_string(),
        generation,
        set: set(3),
        leaving: None,
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
    assert_eq!(elections.overlay(|_| Some(4)).len(), 1);
    assert_eq!(elections.overlay(|_| None).len(), 1);
    assert!(elections.overlay(|_| Some(5)).is_empty());
    assert!(elections.get(&key()).is_none());
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
