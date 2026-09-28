//! The node catalog: registration, heartbeats, expiry and lifecycle moves.
use super::InMemoryStore;
use crate::model::{Node, NodeChange, NodeChangeOp, NodeLifecycle, NodePatchRequest};
use crate::store::{ChangeSet, Snapshot, StoreError, StoreResult};

/// Whether a registration is held to the enabled fleet features.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FleetRule {
    /// Refuse a node lacking an enabled fleet feature.
    Enforce,
    /// Neither check nor store features: the Raft entry from before they
    /// existed, which an older member applies exactly this way.
    Ignore,
}

pub(super) async fn register_node(
    store: &InMemoryStore,
    mut node: Node,
    rule: FleetRule,
) -> StoreResult<Node> {
    node.validate().map_err(invalid_node)?;
    let mut state = store.nodes.write().await;

    match rule {
        // Under the write lock, so a finalize cannot land between the check
        // and the insert.
        FleetRule::Enforce => crate::cluster::fleet::admit(&state.fleet_enabled, &node)?,
        FleetRule::Ignore => node.status.features.clear(),
    }

    if let Some((holder, _)) = state.records.iter().find(|(id, existing)| {
        existing.spec.advertise_addr == node.spec.advertise_addr && *id != &node.node_id
    }) {
        return Err(StoreError::Conflict(format!(
            "advertise_addr {} is already registered to node {holder}",
            node.spec.advertise_addr
        )));
    }

    let stored = match state.records.get(&node.node_id) {
        Some(existing) => {
            if !existing
                .status
                .lifecycle
                .can_transition_to(node.status.lifecycle)
            {
                return Err(invalid_transition(
                    existing.status.lifecycle,
                    node.status.lifecycle,
                ));
            }
            Node {
                status: crate::model::NodeStatus {
                    // The identity outlives the process, so its first
                    // registration is what dates it.
                    registered_at_millis: existing.status.registered_at_millis,
                    incarnation: existing.status.incarnation + 1,
                    ..node.status
                },
                ..node
            }
        }
        None => Node {
            status: crate::model::NodeStatus {
                incarnation: 0,
                ..node.status
            },
            ..node
        },
    };

    state.records.insert(stored.node_id.clone(), stored.clone());
    state.record(
        NodeChangeOp::Registered,
        &stored.node_id,
        Some(stored.clone()),
    );
    metrics::counter!("felix_node_changes_total", "op" => "registered").increment(1);
    Ok(stored)
}

pub(super) async fn finalize_fleet_feature(
    store: &InMemoryStore,
    feature: &str,
) -> StoreResult<std::collections::BTreeSet<String>> {
    let mut state = store.nodes.write().await;
    if !state.fleet_enabled.contains(feature) {
        crate::cluster::fleet::check_finalize(state.records.values(), feature)?;
        state.fleet_enabled.insert(feature.to_string());
    }
    Ok(state.fleet_enabled.clone())
}

pub(super) async fn get_node(store: &InMemoryStore, node_id: &str) -> StoreResult<Node> {
    store
        .nodes
        .read()
        .await
        .records
        .get(node_id)
        .cloned()
        .ok_or_else(|| StoreError::NotFound("node".into()))
}

pub(super) async fn list_nodes(store: &InMemoryStore) -> StoreResult<Vec<Node>> {
    let mut items: Vec<Node> = store.nodes.read().await.records.values().cloned().collect();
    items.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    Ok(items)
}

pub(super) async fn patch_node(
    store: &InMemoryStore,
    node_id: &str,
    patch: NodePatchRequest,
) -> StoreResult<Node> {
    let mut state = store.nodes.write().await;
    let existing = state
        .records
        .get(node_id)
        .ok_or_else(|| StoreError::NotFound("node".into()))?;
    let patched = patch.apply(existing).map_err(invalid_node)?;

    if let Some((holder, _)) = state.records.iter().find(|(id, other)| {
        other.spec.advertise_addr == patched.spec.advertise_addr && *id != node_id
    }) {
        return Err(StoreError::Conflict(format!(
            "advertise_addr {} is already registered to node {holder}",
            patched.spec.advertise_addr
        )));
    }

    state.records.insert(node_id.to_string(), patched.clone());
    state.record(NodeChangeOp::Updated, node_id, Some(patched.clone()));
    metrics::counter!("felix_node_changes_total", "op" => "updated").increment(1);
    Ok(patched)
}

pub(super) async fn delete_node(store: &InMemoryStore, node_id: &str) -> StoreResult<()> {
    // Nodes before shards, the order an assignment write takes them in, and
    // held across the check: released in between, a write could validate
    // this node and land an assignment to it after the count.
    let mut state = store.nodes.write().await;
    if !state.records.contains_key(node_id) {
        return Err(StoreError::NotFound("node".into()));
    }

    // Refused rather than cascaded: deleting the assignment would erase the
    // only record of where that shard's data lives.
    let held = store
        .shards
        .read()
        .await
        .records
        .values()
        .filter(|assignment| assignment.nodes().any(|node| node == node_id))
        .count();
    if held > 0 {
        return Err(StoreError::Conflict(format!(
            "node {node_id} still holds {held} shard(s); reassign them first"
        )));
    }

    if state.records.remove(node_id).is_none() {
        return Err(StoreError::NotFound("node".into()));
    }
    state.record(NodeChangeOp::Deregistered, node_id, None);
    metrics::counter!("felix_node_changes_total", "op" => "deregistered").increment(1);
    Ok(())
}

pub(super) async fn record_node_heartbeat(
    store: &InMemoryStore,
    node_id: &str,
    incarnation: u64,
    at_millis: u64,
    stamp_left: bool,
) -> StoreResult<Node> {
    let mut state = store.nodes.write().await;
    let node = state
        .records
        .get_mut(node_id)
        .ok_or_else(|| StoreError::NotFound("node".into()))?;
    if incarnation < node.status.incarnation {
        return Err(StoreError::Conflict(format!(
            "heartbeat for incarnation {incarnation} of {node_id}, which is now at {}",
            node.status.incarnation
        )));
    }
    // The answer to a node that has left grants no lease, and its stamp is
    // what placement waits out before handing its shards on; a broker still
    // heartbeating after it was deregistered must not keep pushing that back.
    if node.status.lifecycle == NodeLifecycle::Left && !stamp_left {
        return Ok(node.clone());
    }
    // Never moves backwards: heartbeats from two connections can arrive out
    // of order, and the newest observation is the one that matters.
    node.status.last_heartbeat_at_millis = node.status.last_heartbeat_at_millis.max(at_millis);
    Ok(node.clone())
}

pub(super) async fn clamp_future_heartbeats(
    store: &InMemoryStore,
    now_millis: u64,
) -> StoreResult<u64> {
    let mut state = store.nodes.write().await;
    let mut clamped = 0;
    for node in state.records.values_mut() {
        if node.status.last_heartbeat_at_millis > now_millis {
            node.status.last_heartbeat_at_millis = now_millis;
            clamped += 1;
        }
    }
    Ok(clamped)
}

pub(super) async fn expire_stale_nodes(
    store: &InMemoryStore,
    expiry_before_millis: u64,
) -> StoreResult<Vec<Node>> {
    let mut state = store.nodes.write().await;
    let mut stale: Vec<String> = state
        .records
        .values()
        .filter(|node| {
            matches!(
                node.status.lifecycle,
                NodeLifecycle::Live | NodeLifecycle::Draining
            ) && node.status.last_heartbeat_at_millis < expiry_before_millis
        })
        .map(|node| node.node_id.clone())
        .collect();
    // Sorted before publishing, not after returning: each expiry takes a
    // change-log seq here, and a Raft replica applying this command must
    // hand the same node the same seq — HashMap order would not.
    stale.sort();
    Ok(mark_down(&mut state, stale))
}

impl InMemoryStore {
    /// A heartbeat as the Raft log carries it, stamped whatever the node's
    /// lifecycle now. Every replica, older builds included, has to apply an
    /// entry the same way, and a checkpointed beat was granted before the
    /// node left even when the checkpoint lands after.
    pub(crate) async fn apply_node_heartbeat(
        &self,
        node_id: &str,
        incarnation: u64,
        at_millis: u64,
    ) -> StoreResult<Node> {
        record_node_heartbeat(self, node_id, incarnation, at_millis, true).await
    }

    /// Mark exactly `nodes` down, each only while it still serves at the
    /// incarnation given; the Raft leader's expiry decision, applied.
    ///
    /// The incarnation check is what keeps a registration that commits
    /// between the leader's judgement and this apply from being expired for
    /// the silence of the process it replaced.
    pub(crate) async fn expire_nodes(
        &self,
        nodes: &[crate::store::raft::command::NodeIncarnation],
    ) -> Vec<Node> {
        let mut state = self.nodes.write().await;
        let mut stale: Vec<String> = nodes
            .iter()
            .filter(|wanted| {
                state.records.get(&wanted.node_id).is_some_and(|node| {
                    node.status.incarnation == wanted.incarnation
                        && matches!(
                            node.status.lifecycle,
                            NodeLifecycle::Live | NodeLifecycle::Draining
                        )
                })
            })
            .map(|wanted| wanted.node_id.clone())
            .collect();
        stale.sort();
        stale.dedup();
        mark_down(&mut state, stale)
    }
}

/// Move `stale` (sorted, every id present) down, publishing one change each.
fn mark_down(state: &mut super::NodeState, stale: Vec<String>) -> Vec<Node> {
    let mut expired = Vec::with_capacity(stale.len());
    for node_id in stale {
        let node = state.records.get_mut(&node_id).expect("just listed");
        crate::cluster::membership::metrics::record_transition(
            node.status.lifecycle,
            NodeLifecycle::Down,
        );
        node.status.lifecycle = NodeLifecycle::Down;
        let moved = node.clone();
        state.record(NodeChangeOp::Updated, &node_id, Some(moved.clone()));
        metrics::counter!("felix_node_changes_total", "op" => "updated").increment(1);
        expired.push(moved);
    }
    expired
}

pub(super) async fn set_node_lifecycle(
    store: &InMemoryStore,
    node_id: &str,
    lifecycle: NodeLifecycle,
) -> StoreResult<Option<Node>> {
    let mut state = store.nodes.write().await;
    let node = state
        .records
        .get_mut(node_id)
        .ok_or_else(|| StoreError::NotFound("node".into()))?;
    if node.status.lifecycle == lifecycle {
        return Ok(None);
    }
    if !node.status.lifecycle.can_transition_to(lifecycle) {
        return Err(invalid_transition(node.status.lifecycle, lifecycle));
    }
    let previous = node.status.lifecycle;
    node.status.lifecycle = lifecycle;
    let updated = node.clone();
    crate::cluster::membership::metrics::record_transition(previous, lifecycle);
    state.record(NodeChangeOp::Updated, node_id, Some(updated.clone()));
    metrics::counter!("felix_node_changes_total", "op" => "updated").increment(1);
    Ok(Some(updated))
}

pub(super) async fn node_snapshot(store: &InMemoryStore) -> StoreResult<Snapshot<Node>> {
    // One guard, so `items` and `next_seq` describe the same instant.
    let state = store.nodes.read().await;
    let mut items: Vec<Node> = state.records.values().cloned().collect();
    items.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    Ok(Snapshot {
        items,
        next_seq: state.changes.next_seq,
    })
}

pub(super) async fn node_changes(
    store: &InMemoryStore,
    since: u64,
) -> StoreResult<ChangeSet<NodeChange>> {
    let state = store.nodes.read().await;
    let items = state
        .changes
        .items
        .iter()
        .filter(|item| item.seq >= since)
        .take(store.limit())
        .cloned()
        .collect();
    Ok(ChangeSet {
        items,
        next_seq: state.changes.next_seq,
    })
}

/// A model rejection is the caller's fault, so it surfaces as a conflict
/// rather than an internal error.
fn invalid_node(err: crate::model::NodeValidationError) -> StoreError {
    StoreError::Conflict(err.to_string())
}

fn invalid_transition(from: NodeLifecycle, to: NodeLifecycle) -> StoreError {
    invalid_node(crate::model::NodeValidationError::UnsupportedTransition { from, to })
}
