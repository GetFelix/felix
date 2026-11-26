use super::{ClusterConfig, engine_named};

fn engine(name: &str) -> ClusterConfig {
    ClusterConfig {
        broker_env: vec![("FELIX_SHARD_ENGINE".to_string(), name.to_string())],
        ..Default::default()
    }
}

/// **A Raft cluster's start waits for the first hand-overs (#3157).** The
/// broker that forms a group leads its first term and then hands over to
/// placement's leader. A start that returned before that let a test read an
/// owner that was about to move, or publish into the election.
#[test]
fn a_raft_start_waits_for_placed_leaders_unless_the_test_opts_out() {
    assert!(engine("raft").awaits_placed_raft_leaders());
    let pending = ClusterConfig {
        pending_raft_hand_overs: true,
        ..engine("raft")
    };
    assert!(!pending.awaits_placed_raft_leaders());
}

/// The hybrid has no group to hand over, so its start has nothing to wait on.
#[test]
fn a_hybrid_start_waits_for_no_hand_over() {
    assert!(!engine("hybrid").awaits_placed_raft_leaders());
}

#[test]
fn raft_is_the_default_test_engine() {
    assert_eq!(engine_named(None), "raft");
    assert_eq!(engine_named(Some("")), "raft");
    assert_eq!(engine_named(Some("raft")), "raft");
}

#[test]
fn hybrid_is_opt_in() {
    assert_eq!(engine_named(Some("hybrid")), "hybrid");
}

/// **A Raft cluster starts new; a hybrid one starts as before #1774.** The
/// hybrid's tests predate the new-cluster rule, so a hybrid start with seeded
/// streams reads as upgraded unless the test asks for a new cluster.
#[test]
fn a_cluster_starts_new_on_raft_and_upgraded_on_the_hybrid() {
    assert!(!engine("raft").starts_upgraded());
    assert!(engine("hybrid").starts_upgraded());
    let empty = ClusterConfig {
        streams: Vec::new(),
        ..engine("hybrid")
    };
    assert!(
        !empty.starts_upgraded(),
        "nothing seeded was new before #1774"
    );
    let new = ClusterConfig {
        upgraded_fleet: Some(false),
        ..engine("hybrid")
    };
    assert!(!new.starts_upgraded());
    let upgraded = ClusterConfig {
        upgraded_fleet: Some(true),
        ..engine("raft")
    };
    assert!(upgraded.starts_upgraded());
}
