//! Fleet features: which ones every serving broker supports, and which ones
//! an operator has enabled.
//!
//! A broker reports the features it implements when it registers
//! (`felix_common::fleet`). The intersection over live and draining brokers
//! is the *supported* set; a broker that is down or has left no longer
//! counts. Support alone turns nothing on. A feature is *enabled* only when an
//! operator finalizes it ([`check_finalize`]), which is refused unless every
//! serving broker supports it, and brokers act on the enabled set alone.
//!
//! Enabling is one-way. From then on a broker lacking the feature is refused
//! at registration ([`admit`]), so an enabled feature is never withdrawn from
//! under a serving fleet. Before that nothing is refused, which is what lets
//! one broker be rolled back mid-upgrade. Each backend serializes finalizing
//! with registration, so a broker without the feature cannot slip in while
//! it is being enabled.
use std::collections::BTreeSet;

use crate::model::Node;
use crate::store::StoreError;

/// The features every serving node in `nodes` reported.
pub fn supported<'a, I>(nodes: I) -> BTreeSet<String>
where
    I: IntoIterator<Item = &'a Node>,
{
    felix_common::fleet::minimum(
        nodes
            .into_iter()
            .filter(|node| node.status.lifecycle.is_serving())
            .map(|node| &node.status.features),
    )
}

/// The ids of the serving nodes in `nodes` that did not report `feature`.
pub fn lacking<'a, I>(nodes: I, feature: &str) -> Vec<String>
where
    I: IntoIterator<Item = &'a Node>,
{
    nodes
        .into_iter()
        .filter(|node| {
            node.status.lifecycle.is_serving() && !node.status.features.contains(feature)
        })
        .map(|node| node.node_id.clone())
        .collect()
}

/// Refuse `joining` if it lacks a feature the fleet has enabled.
pub(crate) fn admit(enabled: &BTreeSet<String>, joining: &Node) -> Result<(), StoreError> {
    let missing = felix_common::fleet::missing(enabled, &joining.status.features);
    if missing.is_empty() {
        return Ok(());
    }
    Err(StoreError::Conflict(format!(
        "node {} does not support fleet feature(s) {} that the fleet has enabled; \
         run a build that has them, since an enabled feature cannot be disabled",
        joining.node_id,
        missing.join(", "),
    )))
}

/// Refuse to enable `feature` unless some broker is serving and every serving
/// broker in `nodes` reported it.
///
/// With nothing serving, "every broker supports it" is vacuously true and
/// proves nothing about the next broker to start.
pub(crate) fn check_finalize<'a, I>(nodes: I, feature: &str) -> Result<(), StoreError>
where
    I: IntoIterator<Item = &'a Node>,
{
    let mut serving = 0usize;
    let mut lacking = Vec::new();
    for node in nodes {
        if !node.status.lifecycle.is_serving() {
            continue;
        }
        serving += 1;
        if !node.status.features.contains(feature) {
            lacking.push(node.node_id.as_str());
        }
    }
    if serving == 0 {
        return Err(StoreError::Conflict(format!(
            "cannot finalize {feature}: no broker is serving"
        )));
    }
    if lacking.is_empty() {
        return Ok(());
    }
    Err(StoreError::Conflict(format!(
        "cannot finalize {feature}: serving broker(s) {} do not support it; upgrade them first",
        lacking.join(", "),
    )))
}

#[cfg(test)]
mod tests;
