//! A shard whose assignment carries a seal order, or the recorded switch to
//! Raft: closed, never opened or fenced by the hybrid, and reopened only by
//! a later generation.
use felix_replication::driver::WriteFence;
use felix_replication::sealing::{SealOrder, SealOrders};

use super::*;
use crate::shards::watch::Switch;

fn sealing(leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        switch: Some(Switch {
            generation,
            sealer: leader.to_string(),
            sealer_copy: None,
            report: None,
        }),
        ..assigned_to(leader, generation)
    }
}

fn switched(leader: &str, generation: u64, sealed_at: u64) -> ShardAssignment {
    ShardAssignment {
        switch: Some(Switch {
            generation: sealed_at,
            sealer: leader.to_string(),
            sealer_copy: None,
            report: Some(serde_json::json!({"generation": sealed_at, "offset": 9})),
        }),
        ..assigned_to(leader, generation)
    }
}

fn fencing() -> ShardLifecycle {
    let mut own = lifecycle();
    own.fence_promotions();
    own
}

/// A leader serving generation 3.
fn serving() -> ShardLifecycle {
    let mut own = fencing();
    own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    own.opened(&key(0), 3);
    assert!(own.fenced(&key(0), 3));
    own
}

fn order(generation: u64, continues: bool) -> Vec<SealOrder> {
    vec![SealOrder {
        key: key(0),
        generation,
        continues,
    }]
}

/// **A seal order closes the shard and opens nothing.** The ordinary open
/// would write its own generation-start record where the seal's belongs,
/// and would serve a shard the control plane is sealing.
#[test]
fn a_seal_order_closes_the_shard_without_an_open_or_a_fence() {
    let mut own = serving();
    assert!(own.fence().admit(&key(0), 3).is_ok());

    assert_eq!(
        own.observe(&key(0), Some(&sealing("broker-a", 4))),
        Action::None
    );
    assert_eq!(own.phase(&key(0)), Phase::Sealed);
    assert_eq!(own.generation(&key(0)), Some(4));
    assert!(own.servable().is_empty());
    assert!(own.fence().admit(&key(0), 3).is_err());
    assert!(own.fence().admit(&key(0), 4).is_err());
    assert_eq!(own.fence().awaiting_promotion(&key(0)), None);
    assert_eq!(own.fence().orders(), order(4, true));
    assert!(
        !own.fence().withheld(&key(0), 4),
        "the sealer still ships its followers up to the seal's record"
    );

    // Delivered again, with or without the order: it stays closed.
    for again in [sealing("broker-a", 4), assigned_to("broker-a", 4)] {
        assert_eq!(own.observe(&key(0), Some(&again)), Action::None);
        assert_eq!(own.phase(&key(0)), Phase::Sealed);
        assert!(own.fence().admit(&key(0), 4).is_err());
    }
    assert!(!own.fenced(&key(0), 4), "a fence must not open it");
    assert_eq!(own.opened(&key(0), 4), Opened::Stale);
    assert_eq!(own.phase(&key(0)), Phase::Sealed);
}

/// **Only a leader that was serving the generation before seals.** One
/// that restarted, or skipped a generation, may lack a record a follower
/// holds, and the seal's fence takes no tail. It stays closed, ships
/// nothing, and gives the order up.
#[test]
fn a_seal_order_this_process_does_not_continue_is_not_run() {
    let mut restarted = fencing();
    assert_eq!(
        restarted.observe(&key(0), Some(&sealing("broker-a", 4))),
        Action::None
    );
    assert_eq!(restarted.phase(&key(0)), Phase::Sealed);
    assert_eq!(restarted.fence().orders(), order(4, false));
    assert!(restarted.fence().withheld(&key(0), 4));

    let mut skipped = serving();
    skipped.observe(&key(0), Some(&sealing("broker-a", 5)));
    assert_eq!(skipped.fence().orders(), order(5, false));
    assert!(skipped.fence().admit(&key(0), 3).is_err());

    // A seal naming another sealer is not this broker's to run either.
    let mut other = serving();
    other.observe(
        &key(0),
        Some(&ShardAssignment {
            leader: "broker-a".to_string(),
            ..sealing("broker-b", 4)
        }),
    );
    assert_eq!(other.fence().orders(), order(4, false));
}

/// **An accepted report keeps the shard closed for the hybrid.** The record
/// changes in place, at the seal's generation, and holds at every later
/// one: the switch is one way.
#[test]
fn a_recorded_switch_stays_closed_at_every_generation() {
    let mut own = serving();
    own.observe(&key(0), Some(&sealing("broker-a", 4)));

    assert_eq!(
        own.observe(&key(0), Some(&switched("broker-a", 4, 4))),
        Action::None
    );
    assert_eq!(own.phase(&key(0)), Phase::Sealed);
    assert!(own.fence().orders().is_empty());
    assert!(own.fence().withheld(&key(0), 4));
    assert!(own.fence().admit(&key(0), 4).is_err());

    assert_eq!(
        own.observe(&key(0), Some(&switched("broker-a", 6, 4))),
        Action::None
    );
    assert_eq!(own.phase(&key(0)), Phase::Sealed);
    assert!(own.fence().withheld(&key(0), 6));
    assert_eq!(own.fence().awaiting_promotion(&key(0)), None);
    assert!(own.servable().is_empty());

    // A broker that starts after the switch opens nothing for it.
    let mut restarted = fencing();
    assert_eq!(
        restarted.observe(&key(0), Some(&switched("broker-a", 6, 4))),
        Action::None
    );
    assert_eq!(restarted.phase(&key(0)), Phase::Sealed);
    assert!(restarted.fence().withheld(&key(0), 6));
}

/// **A later generation without a seal reopens the shard, fenced.** That
/// is how the control plane ends a seal it gave up.
#[test]
fn a_later_unsealed_generation_reopens_through_the_fence() {
    let mut own = serving();
    own.observe(&key(0), Some(&sealing("broker-a", 4)));

    assert_eq!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 5))),
        Action::Open {
            key: key(0),
            generation: 5,
            new_term: true,
            fence: true,
            incarnation: 0,
        }
    );
    assert!(own.fence().orders().is_empty());
    assert!(!own.fence().withheld(&key(0), 5));
    assert_eq!(own.opened(&key(0), 5), Opened::Fencing);
    assert!(own.fenced(&key(0), 5));
    assert!(own.fence().admit(&key(0), 5).is_ok());
}

/// A sealed shard that moves to another leader is released like any other.
#[test]
fn a_sealed_shard_that_moves_away_is_released() {
    let mut own = serving();
    own.observe(&key(0), Some(&sealing("broker-a", 4)));
    assert_eq!(
        own.observe(&key(0), Some(&assigned_to("broker-b", 5))),
        Action::Release {
            key: key(0),
            generation: 4,
        }
    );
    assert!(own.fence().orders().is_empty());
}

/// An assignment without the field reads as no switch, and one with it
/// round-trips: an older control plane and this broker exchange the same
/// bytes as before.
#[test]
fn the_switch_field_is_optional_on_the_wire() {
    let plain = serde_json::to_value(assigned_to("broker-a", 3)).expect("json");
    assert!(plain.get("switch").is_none());
    let read: ShardAssignment = serde_json::from_value(plain).expect("read");
    assert!(!read.is_sealed());

    let wire = serde_json::json!({
        "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 0,
        "leader": "broker-a", "generation": 4, "state": "active",
        "switch": {"generation": 4, "sealer": "broker-a", "sealer_copy": "00112233445566778899aabbccddeeff"},
    });
    let read: ShardAssignment = serde_json::from_value(wire).expect("read");
    assert!(read.seals_here("broker-a") && read.is_sealed() && !read.is_switched());
}
