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
/// acknowledgement; another process replaces it whatever it reports, so a
/// broker whose data root was wiped is not counted at its predecessor's,
/// even when a restore rewound its node incarnation. Before any restore the
/// incarnation is 0, and a broker that never reported has acknowledged it.
/// One above the incarnation raises the incarnation past it.
pub(crate) async fn run_placement_ack_contract(store: Arc<dyn ControlPlaneStore>) {
    let brokers = ["broker-a".to_string(), "broker-b".to_string()];
    assert_eq!(store.placement_incarnation().await.expect("read"), 0);
    assert!(store.placement_acked_by(&brokers).await.expect("gate"));

    assert!(record(&store, "broker-a", heard(0, 1)).await);
    assert!(!record(&store, "broker-a", heard(0, 1)).await);
    // The next process, its root wiped.
    assert!(record(&store, "broker-a", heard(0, 2)).await);
    assert!(!record(&store, "broker-a", heard(0, 2)).await);
    // A process under an incarnation a restore rewound, below the last one.
    assert!(record(&store, "broker-a", heard(0, 1)).await);
    assert!(record(&store, "broker-b", heard(0, 1)).await);
    let mut all = store.placement_acks().await.expect("list");
    all.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        all,
        vec![
            ("broker-a".to_string(), heard(0, 1)),
            ("broker-b".to_string(), heard(0, 1)),
        ]
    );
    assert_eq!(store.placement_incarnation().await.expect("read"), 0);

    // broker-b acknowledged 4 at a control plane this one was not restored
    // from. Nothing is acknowledged until both have the incarnation past it.
    assert!(record(&store, "broker-b", heard(4, 1)).await);
    assert_eq!(store.placement_incarnation().await.expect("read"), 5);
    assert!(!store.placement_acked_by(&brokers).await.expect("gate"));
    assert!(record(&store, "broker-a", heard(5, 1)).await);
    assert!(!store.placement_acked_by(&brokers).await.expect("gate"));
    assert!(record(&store, "broker-b", heard(5, 1)).await);
    assert!(store.placement_acked_by(&brokers).await.expect("gate"));
    // A process only raises its own.
    assert!(!record(&store, "broker-a", heard(4, 1)).await);
}
