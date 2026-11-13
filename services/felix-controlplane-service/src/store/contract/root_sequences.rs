//! The highest root sequence a broker's heartbeats carried.
use std::sync::Arc;

use crate::model::RootSequence;
use crate::store::ControlPlaneStore;

fn heard(storage_id: char, sequence: u64) -> RootSequence {
    RootSequence {
        storage_id: storage_id.to_string().repeat(32),
        sequence,
    }
}

/// **The highest per storage id, one per broker.** A lower or equal sequence
/// changes nothing and is not a write; a higher one raises it; a report under
/// another storage id replaces it, since a rotation starts the comparison
/// over.
pub(crate) async fn run_root_sequence_contract(store: Arc<dyn ControlPlaneStore>) {
    assert_eq!(store.root_sequence("broker-a").await.expect("read"), None);
    assert!(
        store
            .record_root_sequence("broker-a", heard('a', 5))
            .await
            .expect("first")
    );
    assert!(
        !store
            .record_root_sequence("broker-a", heard('a', 5))
            .await
            .expect("same")
    );
    assert!(
        !store
            .record_root_sequence("broker-a", heard('a', 3))
            .await
            .expect("lower")
    );
    assert_eq!(
        store.root_sequence("broker-a").await.expect("read"),
        Some(heard('a', 5))
    );
    assert!(
        store
            .record_root_sequence("broker-a", heard('a', 9))
            .await
            .expect("higher")
    );
    assert!(
        store
            .record_root_sequence("broker-a", heard('b', 1))
            .await
            .expect("rotated")
    );
    assert!(
        store
            .record_root_sequence("broker-c", heard('c', 2))
            .await
            .expect("another broker")
    );
    assert_eq!(
        store.root_sequence("broker-a").await.expect("read"),
        Some(heard('b', 1))
    );
    let mut all = store.root_sequences().await.expect("list");
    all.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        all,
        vec![
            ("broker-a".to_string(), heard('b', 1)),
            ("broker-c".to_string(), heard('c', 2)),
        ]
    );
}
