//! A leader back from a crash that may have cost it records it acknowledged
//! does not lead from the copy it led before (#1295).
use std::sync::Arc;

use felix_replication::driver::WriteFence;
use felix_replication::unclean::UncleanStart;
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

/// The copy as `broker-a` led it at `generation` before the crash.
async fn led_before_the_crash(storage: &felix_broker::DurableStorage, generation: u64) {
    let log = log(storage);
    log.append(&[bytes::Bytes::from("acked")])
        .await
        .expect("append");
    log.accept_generation(generation, Some("broker-a"))
        .await
        .expect("lead");
}

fn led_with_followers(generation: u64) -> HashMap<ShardKey, ShardAssignment> {
    let mut assignment = assigned_to("broker-a", generation);
    assignment.replicas = vec!["broker-b".to_string(), "broker-c".to_string()];
    HashMap::from([(key(0), assignment)])
}

fn led_alone(generation: u64) -> HashMap<ShardKey, ShardAssignment> {
    HashMap::from([(key(0), assigned_to("broker-a", generation))])
}

/// A lifecycle and store for `broker-a`, started after a crash when
/// `unclean`.
fn node(
    storage: &Arc<felix_broker::DurableStorage>,
    unclean: bool,
) -> (tokio::sync::Mutex<ShardLifecycle>, DurableShardStore) {
    let lifecycle = ShardLifecycle::new("broker-a");
    let store = DurableShardStore::new(Arc::clone(storage)).with_ballots("broker-a");
    if !unclean {
        return (tokio::sync::Mutex::new(lifecycle), store);
    }
    let start = Arc::new(UncleanStart::new("broker-a"));
    lifecycle.fence().bind_unclean_start(Arc::clone(&start));
    (
        tokio::sync::Mutex::new(lifecycle),
        store.with_unclean_start(start),
    )
}

/// **A leader that crashed gives up the generation it led** and does not
/// serve, ship or count toward a mark for it. Nor at the next generation,
/// which the control plane can write naming it before it hears of the crash:
/// opened from the short copy, it would count that copy in its fence.
#[tokio::test]
async fn a_crashed_leader_gives_up_the_generation_it_led() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = storage(dir.path());
    led_before_the_crash(&storage, 4).await;
    let (lifecycle, store) = node(&storage, true);

    reconcile(&lifecycle, &store, &led_with_followers(4)).await;

    let held = lifecycle.lock().await;
    assert_eq!(held.phase(&key(0)), Phase::Failed);
    assert!(log(&storage).gave_up());
    assert!(held.fence().withheld(&key(0), 4));
    drop(held);

    reconcile(&lifecycle, &store, &led_with_followers(5)).await;
    let held = lifecycle.lock().await;
    assert_eq!(held.phase(&key(0)), Phase::Failed);
    assert!(held.fence().withheld(&key(0), 5));
    assert_eq!(log(&storage).accepted_generation(), 4, "nothing taken at 5");
}

/// The copy stays given up after a clean restart: only a newer leader
/// shipping to it gives it a ballot again.
#[tokio::test]
async fn a_given_up_copy_is_not_led_after_a_clean_restart_either() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = storage(dir.path());
    led_before_the_crash(&storage, 4).await;
    let (lifecycle, store) = node(&storage, true);
    reconcile(&lifecycle, &store, &led_with_followers(4)).await;

    let (lifecycle, store) = node(&storage, false);
    reconcile(&lifecycle, &store, &led_with_followers(4)).await;

    let held = lifecycle.lock().await;
    assert_eq!(held.phase(&key(0)), Phase::Failed);
    assert!(held.fence().withheld(&key(0), 4));
}

/// With no followers this broker holds the only copy, and leads it whatever
/// the crash cost it: there is no other to promote.
#[tokio::test]
async fn an_only_copy_is_led_whatever_it_lost() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = storage(dir.path());
    led_before_the_crash(&storage, 4).await;
    let (lifecycle, store) = node(&storage, true);

    reconcile(&lifecycle, &store, &led_alone(4)).await;

    let held = lifecycle.lock().await;
    assert_eq!(held.phase(&key(0)), Phase::Active);
    assert!(!held.fence().withheld(&key(0), 4));
    assert!(!log(&storage).gave_up());
}

/// A clean start leads the copy it led at once, as before.
#[tokio::test]
async fn a_clean_start_leads_the_copy_it_led() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = storage(dir.path());
    led_before_the_crash(&storage, 4).await;
    let (lifecycle, store) = node(&storage, false);

    reconcile(&lifecycle, &store, &led_with_followers(4)).await;

    let held = lifecycle.lock().await;
    assert_eq!(held.phase(&key(0)), Phase::Active);
    assert!(!held.fence().withheld(&key(0), 4));
}

/// A shard this broker comes to lead at a newer generation, with a ballot a
/// newer leader gave its copy, is led as any other.
#[tokio::test]
async fn a_copy_a_newer_leader_shipped_to_is_led() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = storage(dir.path());
    led_before_the_crash(&storage, 4).await;
    log(&storage)
        .accept_generation(5, Some("broker-b"))
        .await
        .expect("follow");
    let (lifecycle, store) = node(&storage, true);

    reconcile(&lifecycle, &store, &led_with_followers(6)).await;

    let held = lifecycle.lock().await;
    assert_eq!(held.phase(&key(0)), Phase::Active);
    assert!(!held.fence().withheld(&key(0), 6));
}
