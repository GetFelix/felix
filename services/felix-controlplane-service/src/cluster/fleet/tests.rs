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
