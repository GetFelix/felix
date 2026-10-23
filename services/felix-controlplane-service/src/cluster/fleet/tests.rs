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
