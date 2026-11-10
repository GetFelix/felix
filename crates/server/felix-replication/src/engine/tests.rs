use std::sync::Arc;

use felix_broker::ConsistencyLevel;

use super::{Engine, Hybrid};
use crate::ShardKey;
use crate::quorum::{QuorumMarks, ShardServing, Unreadable};

fn key() -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: crate::ShardKind::Stream,
    }
}

/// Serves the shard at `generation`, or not at all.
struct ServedAt {
    generation: Option<u64>,
    replicated: bool,
}

impl ShardServing for ServedAt {
    fn replicated(&self, _key: &ShardKey) -> bool {
        self.replicated
    }

    fn generation(&self, _key: &ShardKey) -> Option<u64> {
        self.generation
    }

    fn lease_valid(&self) -> bool {
        true
    }

    fn record_ack_refusal(&self) {}
}

fn hybrid(marks: &Arc<QuorumMarks>, serving: ServedAt) -> Hybrid<ServedAt> {
    Hybrid::new(Some(Arc::clone(marks)), Some(Arc::new(serving)))
}

#[test]
fn the_hybrid_bounds_a_quorum_read_at_the_mark_for_the_served_generation() {
    let quorum = Some(ConsistencyLevel::Quorum);
    let marks = Arc::new(QuorumMarks::new());
    marks.publish(&key(), 3, 5);
    let engine = hybrid(
        &marks,
        ServedAt {
            generation: Some(4),
            replicated: true,
        },
    );

    assert_eq!(engine.read_bound(quorum, &key()), Err(Unreadable::Settling));
    marks.publish(&key(), 4, 7);
    assert_eq!(engine.read_bound(quorum, &key()), Ok(Some(7)));
}

#[test]
fn the_hybrid_leaves_reads_unbounded_where_nothing_is_replicated() {
    let marks = Arc::new(QuorumMarks::new());
    let quorum = Some(ConsistencyLevel::Quorum);

    let unreplicated = hybrid(
        &marks,
        ServedAt {
            generation: Some(1),
            replicated: false,
        },
    );
    assert_eq!(unreplicated.read_bound(quorum, &key()), Ok(None));

    let leader_only = hybrid(
        &marks,
        ServedAt {
            generation: Some(1),
            replicated: true,
        },
    );
    assert_eq!(
        leader_only.read_bound(Some(ConsistencyLevel::Leader), &key()),
        Ok(None)
    );

    let single_node: Hybrid<ServedAt> = Hybrid::new(None, None);
    assert_eq!(single_node.read_bound(quorum, &key()), Ok(None));
}

#[test]
fn the_hybrid_refuses_a_quorum_read_of_a_shard_it_does_not_serve() {
    let marks = Arc::new(QuorumMarks::new());
    marks.publish(&key(), 3, 5);
    let engine = hybrid(
        &marks,
        ServedAt {
            generation: None,
            replicated: true,
        },
    );

    assert_eq!(
        engine.read_bound(Some(ConsistencyLevel::Quorum), &key()),
        Err(Unreadable::NotServed)
    );
}
