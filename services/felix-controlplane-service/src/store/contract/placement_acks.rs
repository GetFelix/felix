//! The placement acknowledgement a broker's heartbeats carried.
use std::sync::Arc;

use crate::model::PlacementAck;
use crate::store::ControlPlaneStore;

fn heard(incarnation: u64, node_incarnation: u64) -> PlacementAck {
    PlacementAck {
        incarnation,
        node_incarnation,
    }
}

async fn record(store: &Arc<dyn ControlPlaneStore>, node: &str, heard: PlacementAck) -> bool {
    store
        .record_placement_ack(node, heard)
        .await
        .expect("record")
}

/// **The highest per process, one per broker.** A process only raises its
/// acknowledgement; a later process replaces it whatever it reports, so a
/// broker whose data root was wiped is not counted at its predecessor's.
/// Before any restore the incarnation is 0, and a broker that never
/// reported has acknowledged it.
pub(crate) async fn run_placement_ack_contract(store: Arc<dyn ControlPlaneStore>) {
    let brokers = ["broker-a".to_string(), "broker-b".to_string()];
    assert_eq!(store.placement_incarnation().await.expect("read"), 0);
    assert!(store.placement_acked_by(&brokers).await.expect("gate"));

    assert!(record(&store, "broker-a", heard(0, 1)).await);
    assert!(!record(&store, "broker-a", heard(0, 1)).await);
    assert!(record(&store, "broker-a", heard(2, 1)).await);
    assert!(!record(&store, "broker-a", heard(1, 1)).await);
    // The next process, its root wiped.
    assert!(record(&store, "broker-a", heard(0, 2)).await);
    // A late beat of the process before it.
    assert!(!record(&store, "broker-a", heard(5, 1)).await);
    assert!(record(&store, "broker-b", heard(1, 1)).await);
    let mut all = store.placement_acks().await.expect("list");
    all.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        all,
        vec![
            ("broker-a".to_string(), heard(0, 2)),
            ("broker-b".to_string(), heard(1, 1)),
        ]
    );
}
