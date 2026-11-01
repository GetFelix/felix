use std::collections::BTreeSet;

use super::*;
use crate::model::NodeLifecycle;
use crate::store::contract::nodes::node;

fn at(id: &str, lifecycle: NodeLifecycle, features: &[&str]) -> Node {
    let mut node = node(id, 7000);
    node.status.lifecycle = lifecycle;
    node.status.features = features.iter().map(|f| f.to_string()).collect();
    node
}

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|f| f.to_string()).collect()
}

#[test]
fn down_and_departed_nodes_leave_the_supported_set() {
    let nodes = [
        at("a", NodeLifecycle::Live, &["x", "y"]),
        at("b", NodeLifecycle::Draining, &["x", "y"]),
        at("c", NodeLifecycle::Down, &[]),
        at("d", NodeLifecycle::Left, &["x"]),
    ];
    assert_eq!(supported(&nodes), set(&["x", "y"]));
}

#[test]
fn an_old_broker_that_reports_nothing_holds_everything_back() {
    let nodes = [
        at("a", NodeLifecycle::Live, &["x"]),
        at("b", NodeLifecycle::Live, &[]),
    ];
    assert!(supported(&nodes).is_empty());
    assert_eq!(lacking(&nodes, "x"), vec!["b".to_string()]);
}

#[test]
fn a_joiner_lacking_an_enabled_feature_is_refused() {
    let refused = admit(&set(&["x"]), &at("c", NodeLifecycle::Live, &["y"]));
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("feature(s) x that")),
        "{refused:?}"
    );
    assert!(admit(&set(&["x"]), &at("c", NodeLifecycle::Live, &["x", "y"])).is_ok());
}

#[test]
fn a_joiner_reading_by_lease_is_told_to_unset_it() {
    let refused = admit(
        &set(&["lease_free_reads"]),
        &at("c", NodeLifecycle::Live, &["majority_ack"]),
    );
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message))
            if message.contains("unset FELIX_QUORUM_READS") && message.contains("restart the broker")),
        "{refused:?}"
    );
    let other = admit(&set(&["x"]), &at("c", NodeLifecycle::Live, &[]));
    assert!(
        matches!(&other, Err(StoreError::Conflict(message)) if !message.contains("FELIX_QUORUM_READS")),
        "{other:?}"
    );
}

#[test]
fn a_joiner_without_the_fence_is_told_to_unset_it() {
    let refused = admit(
        &set(&["generation_start", "majority_ack"]),
        &at("c", NodeLifecycle::Live, &["generation_start"]),
    );
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message))
            if message.contains("unset FELIX_INTERNAL_FENCE")
                && !message.contains("FELIX_QUORUM_READS")),
        "{refused:?}"
    );
}

#[test]
fn nothing_is_refused_before_a_finalize() {
    assert!(admit(&set(&[]), &at("c", NodeLifecycle::Live, &[])).is_ok());
}

#[test]
fn finalizing_needs_every_serving_broker() {
    let nodes = [
        at("a", NodeLifecycle::Live, &["x"]),
        at("b", NodeLifecycle::Draining, &[]),
        at("c", NodeLifecycle::Down, &[]),
    ];
    let refused = check_finalize(&nodes, "x");
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("broker(s) b do")),
        "{refused:?}"
    );
    // A down broker does not hold it back; it is refused when it returns.
    assert!(check_finalize(&nodes[..1], "x").is_ok());
    assert!(check_finalize(&nodes[2..], "x").is_err(), "nothing serving");
}

#[test]
fn follower_features_wait_for_generation_start() {
    let refused = check_prerequisites(&set(&[]), "majority_ack");
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("finalize generation_start first")),
        "{refused:?}"
    );
    let refused = check_prerequisites(&set(&["majority_ack"]), "lease_free_reads");
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("finalize generation_start first")),
        "{refused:?}"
    );
    assert_eq!(
        missing_prerequisites(&set(&["generation_start"]), "lease_free_reads"),
        vec!["majority_ack".to_string()]
    );
    assert!(check_prerequisites(&set(&[]), "generation_start").is_ok());
    assert!(check_prerequisites(&set(&["generation_start"]), "majority_ack").is_ok());
    assert!(
        check_prerequisites(
            &set(&["generation_start", "majority_ack"]),
            "lease_free_reads"
        )
        .is_ok()
    );
    // A fleet that finalized out of order is not refused a repeat.
    assert!(check_prerequisites(&set(&["majority_ack"]), "majority_ack").is_ok());
}

/// A new cluster starts with every feature this build implements but
/// `publisher_principal`; a feature added later fails this until it picks.
#[test]
fn a_new_cluster_leaves_only_publisher_principal_to_the_operator() {
    let manual: Vec<_> = felix_common::fleet::IMPLEMENTED
        .iter()
        .filter(|feature| !NEW_CLUSTER.contains(feature))
        .collect();
    assert_eq!(manual, [&felix_common::fleet::PUBLISHER_PRINCIPAL]);
    assert_eq!(
        NEW_CLUSTER.len() + 1,
        felix_common::fleet::IMPLEMENTED.len()
    );
}

/// Finalized in order, a feature never meets a prerequisite still off.
#[test]
fn a_new_cluster_finalizes_prerequisites_first() {
    let mut enabled = BTreeSet::new();
    for feature in NEW_CLUSTER {
        assert!(
            check_prerequisites(&enabled, feature.name()).is_ok(),
            "{feature} before what it needs"
        );
        enabled.insert(feature.name().to_string());
    }
}

/// Nothing reports the election features yet, so a fleet running this build
/// cannot finalize them even with every prerequisite enabled.
#[test]
fn the_election_features_cannot_be_finalized_yet() {
    let reported: Vec<&str> = felix_common::fleet::IMPLEMENTED
        .iter()
        .map(|feature| feature.name())
        .collect();
    let nodes = [
        at("a", NodeLifecycle::Live, &reported),
        at("b", NodeLifecycle::Live, &reported),
    ];
    for feature in ["joint_fence", "replica_election"] {
        let refused = check_finalize(&nodes, feature);
        assert!(
            matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("do not support it")),
            "{feature}: {refused:?}"
        );
        assert!(!NEW_CLUSTER.iter().any(|f| f.name() == feature));
    }
    // The prerequisites are still enforced in order.
    let refused = check_prerequisites(&set(&["generation_start"]), "replica_election");
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("majority_ack")),
        "{refused:?}"
    );
    assert!(check_prerequisites(&set(&[]), "joint_fence").is_ok());
    assert!(
        check_prerequisites(
            &set(&[
                "generation_start",
                "majority_ack",
                "lease_free_reads",
                "joint_fence"
            ]),
            "replica_election"
        )
        .is_ok()
    );
}

fn offering_election(id: &str, features: &[&str]) -> Node {
    let mut reported = features.to_vec();
    reported.push("peer_election");
    at(id, NodeLifecycle::Live, &reported)
}

fn conflict(result: Result<(), StoreError>) -> String {
    match result {
        Err(StoreError::Conflict(message)) => message,
        other => panic!("expected a conflict, got {other:?}"),
    }
}

#[test]
fn joint_fence_is_refused_while_a_serving_broker_lacks_election() {
    let nodes = [
        offering_election("a", &["joint_fence"]),
        at("b", NodeLifecycle::Live, &["joint_fence"]),
        // Down brokers do not count.
        at("c", NodeLifecycle::Down, &[]),
    ];
    let message = conflict(check_election(&nodes, "joint_fence", false));
    assert!(
        message.contains("broker(s) b do not offer the ELECTION"),
        "{message}"
    );
    assert!(check_election(&nodes[..1], "joint_fence", false).is_ok());
}

#[test]
fn replica_election_is_refused_while_a_serving_broker_lacks_election() {
    let nodes = [
        offering_election("a", &["replica_election"]),
        at("b", NodeLifecycle::Draining, &["replica_election"]),
    ];
    let message = conflict(check_election(&nodes, "replica_election", true));
    assert!(
        message.contains("broker(s) b do not offer the ELECTION"),
        "{message}"
    );
    assert!(check_election(&nodes[..1], "replica_election", true).is_ok());
}

#[test]
fn replica_election_is_refused_below_the_metadata_level() {
    let nodes = [offering_election("a", &["replica_election"])];
    let message = conflict(check_election(&nodes, "replica_election", false));
    assert!(
        message.contains("metadata level") && message.contains("not yet implemented"),
        "{message}"
    );
    // joint_fence needs no metadata level.
    assert!(check_election(&nodes, "joint_fence", false).is_ok());
}

#[test]
fn replica_election_is_refused_until_joint_fence_and_the_rest_are_finalized() {
    let all = ["generation_start", "majority_ack", "lease_free_reads"];
    let message = conflict(check_prerequisites(&set(&all), "replica_election"));
    assert!(message.contains("joint_fence"), "{message}");
    for missing in all {
        let have: Vec<&str> = all
            .iter()
            .copied()
            .filter(|name| *name != missing)
            .chain(["joint_fence"])
            .collect();
        let message = conflict(check_prerequisites(&set(&have), "replica_election"));
        assert!(message.contains(missing), "{message}");
    }
    let mut ready = all.to_vec();
    ready.push("joint_fence");
    assert!(check_prerequisites(&set(&ready), "replica_election").is_ok());
}

#[test]
fn other_features_pass_the_election_check() {
    let nodes = [at("a", NodeLifecycle::Live, &["majority_ack"])];
    assert!(check_election(&nodes, "majority_ack", false).is_ok());
}
