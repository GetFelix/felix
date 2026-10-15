//! What a broker last said about the leaders it cannot reach.
use std::sync::Arc;

use crate::model::NodeSuspicion;
use crate::store::ControlPlaneStore;

fn suspicion(node_id: &str, suspects: &[&str], at: u64) -> NodeSuspicion {
    NodeSuspicion {
        node_id: node_id.to_string(),
        incarnation: 1,
        suspects: suspects.iter().map(|s| s.to_string()).collect(),
        reported_at_millis: at,
    }
}

/// **One suspicion per broker, the latest.** A broker that suspects someone
/// else, or nobody, replaces what it said before rather than adding to it.
pub(crate) async fn run_suspicion_contract(store: Arc<dyn ControlPlaneStore>) {
    store
        .record_suspicion(suspicion("broker-b", &["broker-a"], 10))
        .await
        .expect("record");
    store
        .record_suspicion(suspicion("broker-c", &["broker-a"], 11))
        .await
        .expect("record");
    store
        .record_suspicion(suspicion("broker-b", &["broker-d"], 12))
        .await
        .expect("replace");

    let mut held = store.list_suspicions().await.expect("list");
    held.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    assert_eq!(held.len(), 2, "{held:?}");
    assert_eq!(held[0].node_id, "broker-b");
    assert_eq!(
        held[0].suspects,
        ["broker-d".to_string()].into_iter().collect()
    );
    assert_eq!(held[1].node_id, "broker-c");
    assert!(held[1].suspects.contains("broker-a"));
}
