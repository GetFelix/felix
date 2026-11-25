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
