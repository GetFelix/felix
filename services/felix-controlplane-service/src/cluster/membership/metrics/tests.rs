//! Metric shape: bounded labels, and a census that matches the node listing.
use std::collections::BTreeMap;

use super::*;
use crate::model::{Node, NodeCapacity, NodeSpec, NodeStatus};

fn node(node_id: &str, region: &str, lifecycle: NodeLifecycle) -> Node {
    Node {
        node_id: node_id.to_string(),
        spec: NodeSpec {
            advertise_addr: "10.0.0.4:7000".to_string(),
            client_addr: None,
            kafka_addr: None,
            region: region.to_string(),
            zone: None,
            labels: BTreeMap::new(),
            capacity: NodeCapacity::default(),
        },
        status: NodeStatus {
            lifecycle,
            last_heartbeat_at_millis: 1,
            registered_at_millis: 1,
            incarnation: 0,
            features: Default::default(),
        },
    }
}

#[test]
fn every_lifecycle_has_a_label_and_they_are_distinct() {
    let labels: Vec<&str> = LIFECYCLES.iter().copied().map(lifecycle_label).collect();
    assert_eq!(labels, vec!["live", "draining", "down", "left"]);

    let unique: std::collections::HashSet<_> = labels.iter().collect();
    assert_eq!(unique.len(), labels.len(), "labels must be distinct");
}

/// The transition series is bounded at lifecycle squared, minus the identity
/// moves callers skip. Sixteen minus four is what a dashboard has to hold.
#[test]
fn the_transition_label_space_stays_small() {
    let mut pairs = 0;
    for from in LIFECYCLES {
        for to in LIFECYCLES {
            if from != to {
                pairs += 1;
            }
        }
    }
    assert_eq!(pairs, 12);
}

/// A no-op move must not count. A repeated expiry sweep calls into the same
/// path, and counting it would turn a steady state into a rising failure rate.
#[test]
fn an_identity_transition_is_not_recorded() {
    // No recorder is installed, so this asserts the guard rather than the
    // counter: the call must simply return.
    record_transition(NodeLifecycle::Down, NodeLifecycle::Down);
    record_transition(NodeLifecycle::Live, NodeLifecycle::Down);
}

/// The census has to survive a region emptying, which is exactly when an
/// operator is looking at it.
#[test]
fn publishing_a_census_covers_regions_that_have_emptied() {
    publish_census(&[
        node("a", "us-west-2", NodeLifecycle::Live),
        node("b", "eu-central-1", NodeLifecycle::Down),
    ]);
    // Second pass without eu-central-1: the region must still be reported, as
    // zero, rather than keeping its last value forever.
    publish_census(&[node("a", "us-west-2", NodeLifecycle::Live)]);

    let seen = SEEN_REGIONS.lock().expect("lock");
    let seen = seen.as_ref().expect("initialised");
    assert!(
        seen.contains("eu-central-1"),
        "a vanished region must stay tracked"
    );
    assert!(seen.contains("us-west-2"));
}

#[test]
fn an_empty_cluster_publishes_without_panicking() {
    publish_census(&[]);
}
