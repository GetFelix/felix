//! Continuity and apply rules: every branch here is a way to silently lose an
//! ownership change.

use super::*;
use crate::shards::ShardKind;

fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard,
        kind: crate::shards::ShardKind::Stream,
    }
}

fn assignment(shard: u32, leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        key: key(shard),
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation,
        state: "active".to_string(),
        successor: None,
        routing: Default::default(),
    }
}

#[test]
fn a_contiguous_page_needs_no_resync() {
    // Asked from 5, got 5 onwards.
    assert_eq!(check_continuity(5, Some(5), 9), None);
    // Asked from 5, nothing new yet.
    assert_eq!(check_continuity(5, None, 5), None);
    // First poll of an empty cluster.
    assert_eq!(check_continuity(0, None, 0), None);
}

/// The change we asked for is gone from the window, so everything between our
/// checkpoint and the first returned change was evicted.
#[test]
fn a_first_seq_above_the_checkpoint_is_a_gap() {
    assert_eq!(check_continuity(5, Some(9), 20), Some(Resync::GapInHistory),);
}

/// The subtle one: an empty page is only safe when the log has not moved. If it
/// has, the whole span was evicted rather than being empty, and resuming would
/// skip it in silence.
#[test]
fn an_empty_page_with_a_moved_log_is_a_gap() {
    assert_eq!(check_continuity(5, None, 12), Some(Resync::GapInHistory));
    assert_eq!(
        check_continuity(5, None, 5),
        None,
        "not moved: genuinely empty"
    );
}

/// A control plane restarted onto a store that does not persist its sequence,
/// so the log is now behind our checkpoint. Continuing would apply old changes
/// as if they were new.
#[test]
fn a_log_behind_the_checkpoint_is_a_reset() {
    assert_eq!(check_continuity(40, None, 0), Some(Resync::SequenceReset));
    assert_eq!(
        check_continuity(40, Some(0), 3),
        Some(Resync::SequenceReset),
        "a reset is checked before a gap, since the gap test is meaningless here",
    );
}

#[test]
fn applying_a_newer_generation_takes_effect() {
    let mut owned = ShardOwnership::default();
    assert!(owned.apply(&key(0), Some(assignment(0, "broker-a", 0))));
    assert!(owned.apply(&key(0), Some(assignment(0, "broker-b", 1))));
    assert_eq!(owned.get(&key(0)).expect("present").leader, "broker-b");
}

/// Duplicate delivery has to be harmless, or an at-least-once poll would need
/// exactly-once plumbing it does not have.
#[test]
fn a_duplicate_change_is_ignored() {
    let mut owned = ShardOwnership::default();
    assert!(owned.apply(&key(0), Some(assignment(0, "broker-a", 3))));
    assert!(!owned.apply(&key(0), Some(assignment(0, "broker-a", 3))));
    assert_eq!(owned.len(), 1);
}

/// The property that makes reordering safe: a change that would move ownership
/// backwards is dropped, not applied.
#[test]
fn a_stale_generation_cannot_roll_ownership_back() {
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(assignment(0, "broker-b", 5)));
    assert!(!owned.apply(&key(0), Some(assignment(0, "broker-a", 4))));
    assert_eq!(
        owned.get(&key(0)).expect("present").leader,
        "broker-b",
        "an older generation must not take the shard back",
    );
}

#[test]
fn an_unassignment_removes_the_shard() {
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(assignment(0, "broker-a", 0)));
    assert!(owned.apply(&key(0), None));
    assert!(owned.get(&key(0)).is_none());
    // Removing it twice is harmless, and reports that nothing changed.
    assert!(!owned.apply(&key(0), None));
}

#[test]
fn a_snapshot_replaces_everything() {
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(assignment(0, "broker-a", 0)));
    owned.apply(&key(1), Some(assignment(1, "broker-a", 0)));

    owned.reset(vec![assignment(2, "broker-c", 7)]);
    assert_eq!(owned.len(), 1);
    assert!(
        owned.get(&key(0)).is_none(),
        "stale ownership must not survive"
    );
    assert_eq!(owned.get(&key(2)).expect("present").leader, "broker-c");
}

#[test]
fn leadership_is_reported_for_this_node_only() {
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(assignment(0, "broker-a", 0)));
    assert!(owned.is_leader(&key(0), "broker-a"));
    assert!(!owned.is_leader(&key(0), "broker-b"));
    assert!(!owned.is_leader(&key(9), "broker-a"), "unknown shard");
}

#[test]
fn backoff_grows_and_then_stops_growing() {
    let interval = Duration::from_millis(100);
    assert_eq!(backoff(interval, 1), interval);
    assert_eq!(backoff(interval, 2), interval * 2);
    assert_eq!(backoff(interval, 3), interval * 4);
    assert_eq!(backoff(interval, 40), MAX_BACKOFF);
}

// --- Cache shards are placed but not yet routed ------------------------------

fn cache_assignment(shard: u32, leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            // Deliberately the stream's name: the two are told apart by kind
            // alone, and a broker that ignored it would file one under the
            // other and serve requests against the wrong log.
            stream: "orders".to_string(),
            shard,
            kind: ShardKind::Cache,
        },
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation,
        state: "active".to_string(),
        successor: None,
        routing: Default::default(),
    }
}

/// A snapshot carrying cache shards keeps them. They were dropped while nothing
/// routed a cache request; now that the data path resolves cache keys through
/// this table, dropping one would make every key in that shard unroutable.
#[test]
fn a_snapshot_keeps_cache_shards() {
    let mut owned = ShardOwnership::default();
    owned.reset(vec![
        assignment(0, "broker-a", 1),
        cache_assignment(0, "broker-b", 1),
        assignment(1, "broker-a", 1),
    ]);

    assert_eq!(owned.len(), 3);
    assert_eq!(
        owned.get(&key(0)).map(|a| a.leader.as_str()),
        Some("broker-a"),
        "the cache shard must not have taken the stream shard's row",
    );
    assert_eq!(
        owned
            .get(&cache_assignment(0, "broker-b", 1).key)
            .map(|a| a.leader.as_str()),
        Some("broker-b"),
    );
}

/// The key includes the kind, so the two are distinct entries rather than one
/// overwriting the other — which is what lets a broker hold a cache shard and a
/// stream shard of the same name and route each to its own log.
#[test]
fn a_cache_key_is_not_the_stream_key_of_the_same_name() {
    let mut owned = ShardOwnership::default();
    assert!(owned.apply(&key(0), Some(assignment(0, "broker-a", 1))));

    let cache = cache_assignment(0, "broker-b", 1);
    assert!(owned.apply(&cache.key.clone(), Some(cache)));

    assert_eq!(owned.len(), 2);
    assert_eq!(
        owned.get(&key(0)).map(|a| a.leader.as_str()),
        Some("broker-a")
    );
}

/// A control plane that predates cache placement sends no kind at all, and the
/// only safe reading of that silence is the kind everything used to be.
#[test]
fn an_assignment_without_a_kind_is_a_stream() {
    let json = r#"{
        "tenant_id": "t1",
        "namespace": "ns",
        "stream": "orders",
        "shard": 3,
        "leader": "broker-a",
        "generation": 7,
        "state": "active"
    }"#;
    let assignment: ShardAssignment = serde_json::from_str(json).expect("legacy assignment");

    assert_eq!(assignment.key.kind, ShardKind::Stream);
    assert_eq!(assignment.key.shard, 3);
    assert_eq!(
        assignment.routing,
        felix_wire::routing::ShardRouting::Modulo,
        "a control plane that predates routing only ever had modulo streams"
    );
}

#[test]
fn an_assignment_carries_its_streams_routing() {
    let json = r#"{
        "tenant_id": "t1",
        "namespace": "ns",
        "stream": "orders",
        "shard": 3,
        "leader": "broker-a",
        "generation": 7,
        "state": "active",
        "routing": "jump_hash"
    }"#;
    let assignment: ShardAssignment = serde_json::from_str(json).expect("assignment");
    assert_eq!(
        assignment.routing,
        felix_wire::routing::ShardRouting::JumpHash
    );
}
