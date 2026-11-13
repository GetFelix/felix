//! The highest root sequence a broker's heartbeats carried.
use std::sync::Arc;

use crate::model::RootSequence;
use crate::store::ControlPlaneStore;

fn heard(storage_id: char, sequence: u64, incarnation: u64) -> RootSequence {
    RootSequence {
        storage_id: storage_id.to_string().repeat(32),
        sequence,
        incarnation,
    }
}

async fn record(store: &Arc<dyn ControlPlaneStore>, node: &str, heard: RootSequence) -> bool {
    store
        .record_root_sequence(node, heard)
        .await
        .expect("record")
}

/// **The highest per storage id, one per broker.** A lower or equal sequence
/// changes nothing and is not a write; a higher one raises it, whatever the
/// incarnation. Another storage id replaces it only from a later
/// incarnation: a late beat of the process before a rotation never brings
/// back the old id at a sequence its root has since passed.
pub(crate) async fn run_root_sequence_contract(store: Arc<dyn ControlPlaneStore>) {
    assert_eq!(store.root_sequence("broker-a").await.expect("read"), None);
    assert!(record(&store, "broker-a", heard('a', 5, 1)).await);
    assert!(!record(&store, "broker-a", heard('a', 5, 1)).await);
    assert!(!record(&store, "broker-a", heard('a', 3, 1)).await);
    assert_eq!(
        store.root_sequence("broker-a").await.expect("read"),
        Some(heard('a', 5, 1))
    );
    assert!(record(&store, "broker-a", heard('a', 9, 1)).await);
    // Rotated by the next process.
    assert!(record(&store, "broker-a", heard('b', 0, 2)).await);
    // A late beat of the process before it.
    assert!(!record(&store, "broker-a", heard('a', 10, 1)).await);
    assert!(!record(&store, "broker-a", heard('c', 10, 2)).await);
    assert!(record(&store, "broker-a", heard('b', 1, 2)).await);
    assert!(record(&store, "broker-c", heard('c', 2, 1)).await);
    assert_eq!(
        store.root_sequence("broker-a").await.expect("read"),
        Some(heard('b', 1, 2))
    );
    let mut all = store.root_sequences().await.expect("list");
    all.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        all,
        vec![
            ("broker-a".to_string(), heard('b', 1, 2)),
            ("broker-c".to_string(), heard('c', 2, 1)),
        ]
    );
}
