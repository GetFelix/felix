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

async fn register(store: &Arc<dyn ControlPlaneStore>, node: &str, port: u16) -> u64 {
    store
        .register_node(super::nodes::node(node, port))
        .await
        .expect("register")
        .status
        .incarnation
}

/// **The highest per process, one per broker, from its current process.**
/// A process only raises its acknowledgement. Only the process the node
/// record holds is heard, and the gate counts only its acknowledgement, so a
/// late beat of an earlier process changes nothing and a broker that
/// registered again (its root perhaps wiped) is not counted at its
/// predecessor's. Before any restore the incarnation is 0, and a broker that
/// never reported has acknowledged it. One above the incarnation raises the
/// incarnation past it, unless it is out of bound.
pub(crate) async fn run_placement_ack_contract(store: Arc<dyn ControlPlaneStore>) {
    let (a, b) = ("broker-a", "broker-b");
    let brokers = [a.to_string(), b.to_string()];
    let a0 = register(&store, a, 7801).await;
    let b0 = register(&store, b, 7802).await;
    assert_eq!(store.placement_incarnation().await.expect("read"), 0);
    assert!(store.placement_acked_by(&brokers).await.expect("gate"));

    assert!(record(&store, a, heard(0, a0)).await);
    assert!(!record(&store, a, heard(0, a0)).await);
    // Not the process the record holds.
    assert!(!record(&store, a, heard(0, a0 + 1)).await);
    assert!(!record(&store, "broker-unknown", heard(0, 0)).await);
    assert!(record(&store, b, heard(0, b0)).await);
    let mut all = store.placement_acks().await.expect("list");
    all.sort_by(|x, y| x.0.cmp(&y.0));
    assert_eq!(
        all,
        vec![(a.to_string(), heard(0, a0)), (b.to_string(), heard(0, b0))]
    );
    assert_eq!(store.placement_incarnation().await.expect("read"), 0);

    // b acknowledged 4 at a control plane this one was not restored from.
    // Nothing is acknowledged until both have the incarnation past it.
    assert!(record(&store, b, heard(4, b0)).await);
    assert_eq!(store.placement_incarnation().await.expect("read"), 5);
    assert!(!store.placement_acked_by(&brokers).await.expect("gate"));
    assert!(record(&store, a, heard(5, a0)).await);
    assert!(!store.placement_acked_by(&brokers).await.expect("gate"));
    assert!(record(&store, b, heard(5, b0)).await);
    assert!(store.placement_acked_by(&brokers).await.expect("gate"));
    // A process only raises its own.
    assert!(!record(&store, a, heard(4, a0)).await);

    // a registers again, its root wiped.
    let a1 = register(&store, a, 7801).await;
    assert!(!store.placement_acked_by(&brokers).await.expect("gate"));
    // A late beat of the earlier process.
    assert!(!record(&store, a, heard(5, a0)).await);
    assert!(record(&store, a, heard(5, a1)).await);
    assert!(store.placement_acked_by(&brokers).await.expect("gate"));

    // Deleted and registered again: the new process starts at 0 again and
    // must not match the deleted one's acknowledgement.
    let c = "broker-c";
    let only_c = [c.to_string()];
    let c0 = register(&store, c, 7803).await;
    assert!(record(&store, c, heard(5, c0)).await);
    assert!(store.placement_acked_by(&only_c).await.expect("gate"));
    store.delete_node(c).await.expect("delete");
    assert_eq!(register(&store, c, 7803).await, c0);
    assert!(!store.placement_acked_by(&only_c).await.expect("gate"));
    assert!(record(&store, c, heard(5, c0)).await);
    assert!(store.placement_acked_by(&only_c).await.expect("gate"));

    // Far enough above to walk the incarnation to its limit: refused.
    let bound = felix_storage::raft_copy::MAX_PLACEMENT_ACK_AHEAD;
    assert!(!record(&store, b, heard(u64::MAX, b0)).await);
    assert!(!record(&store, b, heard(5 + bound + 1, b0)).await);
    assert_eq!(store.placement_incarnation().await.expect("read"), 5);
    assert!(record(&store, b, heard(5 + bound, b0)).await);
    assert_eq!(
        store.placement_incarnation().await.expect("read"),
        5 + bound + 1
    );
}
