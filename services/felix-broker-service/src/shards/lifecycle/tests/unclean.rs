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
    let (lifecycle, store, _) = node_with(storage, unclean);
    (lifecycle, store)
}

/// [`node`], with what it knows of the crash when `unclean`.
fn node_with(
    storage: &Arc<felix_broker::DurableStorage>,
    unclean: bool,
) -> (
    tokio::sync::Mutex<ShardLifecycle>,
    DurableShardStore,
    Option<Arc<UncleanStart>>,
) {
    let lifecycle = ShardLifecycle::new("broker-a");
    let store = DurableShardStore::new(Arc::clone(storage))
        .with_ballots("broker-a")
        .with_fence(Arc::clone(lifecycle.fence()));
    if !unclean {
        return (tokio::sync::Mutex::new(lifecycle), store, None);
    }
    let start = Arc::new(UncleanStart::new("broker-a"));
    lifecycle.fence().bind_unclean_start(Arc::clone(&start));
    (
        tokio::sync::Mutex::new(lifecycle),
        store.with_unclean_start(Arc::clone(&start)),
        Some(start),
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

/// A shard this broker comes to lead at a newer generation, once a newer
/// leader has shipped to its copy in this process, is led as any other.
#[tokio::test]
async fn a_copy_a_newer_leader_shipped_to_is_led() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = storage(dir.path());
    led_before_the_crash(&storage, 4).await;
    log(&storage)
        .accept_generation(5, Some("broker-b"))
        .await
        .expect("follow");
    let (lifecycle, store, start) = node_with(&storage, true);
    start.expect("unclean").vouch(&key(0));

    reconcile(&lifecycle, &store, &led_with_followers(6)).await;

    let held = lifecycle.lock().await;
    assert_eq!(held.phase(&key(0)), Phase::Active);
    assert!(!held.fence().withheld(&key(0), 6));
}

/// **A follower named leader that crashed before it took the generation does
/// not lead its copy** (#1295). It acknowledged records as a follower that
/// the crash may have cost it, and its ballot names the old leader, so its
/// copy is not one it led. Until a leader has shipped to it in this process,
/// leading from it would count it in the fence.
#[tokio::test]
async fn a_follower_named_leader_does_not_lead_an_unvouched_copy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = storage(dir.path());
    let followed = log(&storage);
    followed
        .append(&[bytes::Bytes::from("acked")])
        .await
        .expect("append");
    followed
        .accept_generation(4, Some("broker-b"))
        .await
        .expect("follow");
    let (lifecycle, store) = node(&storage, true);

    reconcile(&lifecycle, &store, &led_with_followers(5)).await;

    let held = lifecycle.lock().await;
    assert_eq!(held.phase(&key(0)), Phase::Failed);
    assert!(held.fence().withheld(&key(0), 5));
    assert_eq!(log(&storage).accepted_generation(), 4, "nothing taken at 5");
}

/// A copy never accepted from anyone has nothing a crash could have cost
/// this broker: a new shard placed here after the crash is led at once.
#[tokio::test]
async fn a_new_shard_is_led_after_an_unclean_start() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = storage(dir.path());
    let (lifecycle, store) = node(&storage, true);

    reconcile(&lifecycle, &store, &led_with_followers(5)).await;

    assert_eq!(lifecycle.lock().await.phase(&key(0)), Phase::Active);
}

/// **A given-up copy is reclaimed at a newer generation once a probe found
/// no follower ahead of it** (#1295): nothing a client was promised is held
/// beyond it. Not at the generation given up, and not on a probe that found
/// one ahead.
#[tokio::test]
async fn a_given_up_copy_no_follower_is_ahead_of_is_reclaimed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = storage(dir.path());
    led_before_the_crash(&storage, 4).await;
    let (lifecycle, store) = node(&storage, true);
    reconcile(&lifecycle, &store, &led_with_followers(4)).await;
    assert_eq!(lifecycle.lock().await.phase(&key(0)), Phase::Failed);

    let fence = Arc::clone(lifecycle.lock().await.fence());
    fence.probed(&key(0), 4, false);
    reconcile(&lifecycle, &store, &led_with_followers(5)).await;
    assert_eq!(
        lifecycle.lock().await.phase(&key(0)),
        Phase::Failed,
        "a follower is ahead"
    );

    fence.probed(&key(0), 4, true);
    reconcile(&lifecycle, &store, &led_with_followers(6)).await;
    let held = lifecycle.lock().await;
    assert_eq!(held.phase(&key(0)), Phase::Active);
    assert!(!held.fence().withheld(&key(0), 6));
    assert!(!log(&storage).gave_up());
    assert_eq!(log(&storage).accepted_leader().as_deref(), Some("broker-a"));
}

/// A probe proves nothing about the generation given up itself, which stays
/// given up.
#[tokio::test]
async fn a_reclaim_never_reopens_the_given_up_generation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = storage(dir.path());
    led_before_the_crash(&storage, 4).await;
    let (lifecycle, store) = node(&storage, true);
    reconcile(&lifecycle, &store, &led_with_followers(4)).await;
    lifecycle.lock().await.fence().probed(&key(0), 4, true);

    let (lifecycle, store) = node(&storage, false);
    lifecycle.lock().await.fence().probed(&key(0), 4, true);
    reconcile(&lifecycle, &store, &led_with_followers(4)).await;
    assert_eq!(lifecycle.lock().await.phase(&key(0)), Phase::Failed);
}

/// With no log this store can reach (a cache without the broker's stores),
/// the shard is still marked led, or it would stay withheld for the life of
/// the process.
#[tokio::test]
async fn a_shard_without_a_reachable_log_is_not_left_withheld() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = storage(dir.path());
    let (_lifecycle, store, start) = node_with(&storage, true);
    let mut cache = key(0);
    cache.kind = crate::shards::ShardKind::Cache;

    store
        .may_lead(&cache, 3, 0, true)
        .await
        .expect("nothing to give up");
    assert!(start.expect("unclean").leads_at(&cache, 3));
}
