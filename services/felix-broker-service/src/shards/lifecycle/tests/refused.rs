//! A leader named at a generation it promised to another node, or below one
//! it already accepted, says so instead of failing quietly.
use std::sync::Arc;

use felix_common::membership::RefusedGeneration;
use felix_replication::driver::WriteFence;
use felix_storage::log::{FsyncMode, LogConfig};

use super::*;

fn storage(dir: &std::path::Path) -> Arc<felix_broker::DurableStorage> {
    Arc::new(
        felix_broker::DurableStorage::open(
            dir,
            LogConfig {
                segment_size_bytes: 4 * 1024,
                index_spacing_bytes: 256,
                fsync_mode: FsyncMode::None,
                preallocate_segments: false,
                ..LogConfig::default()
            },
        )
        .expect("storage"),
    )
}

fn log(storage: &felix_broker::DurableStorage) -> felix_broker::StreamLog {
    storage
        .open_stream(&key(0).tenant_id, &key(0).namespace, &key(0).stream, 0)
        .expect("open")
}

fn led_with_followers(generation: u64) -> HashMap<ShardKey, ShardAssignment> {
    let mut assignment = assigned_to("broker-a", generation);
    assignment.replicas = vec!["broker-b".to_string(), "broker-c".to_string()];
    HashMap::from([(key(0), assignment)])
}

fn node(
    storage: &Arc<felix_broker::DurableStorage>,
) -> (tokio::sync::Mutex<ShardLifecycle>, DurableShardStore) {
    let lifecycle = ShardLifecycle::new("broker-a");
    let store = DurableShardStore::new(Arc::clone(storage))
        .with_ballots("broker-a")
        .with_fence(Arc::clone(lifecycle.fence()));
    (tokio::sync::Mutex::new(lifecycle), store)
}

/// **The refusal is recorded with its generation and the node it was promised
/// to.** Without it the control plane, which recorded that node's election
/// without adopting it and still names this broker at the generation, hears
/// nothing from the leader it is waiting on.
#[tokio::test]
async fn a_promised_generation_is_recorded_as_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = storage(dir.path());
    log(&storage)
        .accept_generation(4, Some("broker-b"))
        .await
        .expect("promise");
    let (lifecycle, store) = node(&storage);

    reconcile(&lifecycle, &store, &led_with_followers(4)).await;

    let held = lifecycle.lock().await;
    assert_eq!(held.phase(&key(0)), Phase::Failed);
    assert_eq!(
        held.fence().refused_at(&key(0)),
        Some(RefusedGeneration {
            generation: 4,
            promised_to: "broker-b".to_string(),
        }),
    );
    drop(held);

    // A generation above the promise opens, and the refusal is cleared.
    reconcile(&lifecycle, &store, &led_with_followers(5)).await;
    let held = lifecycle.lock().await;
    assert_eq!(held.phase(&key(0)), Phase::Active);
    assert_eq!(held.fence().refused_at(&key(0)), None);
}

/// **A generation below one this broker already accepted is recorded as
/// refused, naming no node** (#1914). With elections the newer ballot can be
/// this broker's own standing that never opened, which no assignment names;
/// unreported, the control plane keeps naming it at a generation it can
/// never open. When the control plane did name the newer one, the report is
/// for an older generation than it holds and changes nothing.
#[tokio::test]
async fn a_superseded_generation_is_recorded_as_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = storage(dir.path());
    log(&storage)
        .accept_generation(6, Some("broker-a"))
        .await
        .expect("accept");
    let (lifecycle, store) = node(&storage);

    reconcile(&lifecycle, &store, &led_with_followers(4)).await;

    let held = lifecycle.lock().await;
    assert_eq!(held.phase(&key(0)), Phase::Failed);
    assert_eq!(
        held.fence().refused_at(&key(0)),
        Some(RefusedGeneration::set_unknown(4)),
    );
    drop(held);

    // Named again above the ballot, it opens and the refusal is cleared.
    reconcile(&lifecycle, &store, &led_with_followers(7)).await;
    let held = lifecycle.lock().await;
    assert_eq!(held.phase(&key(0)), Phase::Active);
    assert_eq!(held.fence().refused_at(&key(0)), None);
}
