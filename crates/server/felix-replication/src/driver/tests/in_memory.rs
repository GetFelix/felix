//! An in-memory stream's shard keeps nothing on disk, here or on its
//! followers, and still reports, so a move of it can finish (#3071).

use super::*;
use crate::driver::{Unfenced, WriteFence};

/// A broker with durable storage, and nothing on it for the stream yet.
async fn broker_with_stream(durable: bool) -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            felix_broker::StreamMetadata {
                durable,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    (broker, dir)
}

/// Every path under `dir` that names the stream.
fn paths_naming_stream(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.to_string_lossy().contains(STREAM) {
                found.push(path.clone());
            }
            if path.is_dir() {
                stack.push(path);
            }
        }
    }
    found
}

async fn pass(
    follower: &AcceptingFollower,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    fence: &dyn WriteFence,
) -> Pass {
    pass_with(follower, broker, router, fence, &mut HashMap::default()).await
}

/// [`pass`], carrying the shard's cursors from one pass to the next.
async fn pass_with(
    follower: &AcceptingFollower,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    fence: &dyn WriteFence,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
) -> Pass {
    replicate_once_with(
        follower,
        broker,
        router,
        fence,
        &crate::promotion::NoGate,
        &QuorumMarks::new(),
        None,
        cursors,
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
        &Rebuilds::disabled(),
        &MoveThrottle::unlimited(),
    )
    .await
}

/// **No caller can open an in-memory stream's log.** The broker refuses
/// every open of it, with or without a base and an incarnation, so nothing
/// that resolves a shard's log (the replication pass, a follower, an
/// election, a reclaim) can create one on disk.
#[tokio::test]
async fn an_in_memory_streams_log_does_not_open() {
    let (broker, dir) = broker_with_stream(false).await;
    let kind = felix_broker::LogKind::Stream;

    assert!(
        broker
            .shard_log(kind, TENANT, NAMESPACE, STREAM, 0)
            .await
            .is_none()
    );
    assert!(
        broker
            .shard_log_at(kind, TENANT, NAMESPACE, STREAM, 0, 5)
            .await
            .is_none()
    );
    let incarnation = broker.stream_incarnation(TENANT, NAMESPACE, STREAM);
    assert!(
        broker
            .shard_log_for(kind, TENANT, NAMESPACE, STREAM, 0, Some(0), incarnation)
            .await
            .is_none()
    );
    assert!(
        broker
            .shard_log_off_runtime(kind, TENANT, NAMESPACE, STREAM, 0, Some(0), incarnation)
            .await
            .is_none()
    );
    assert_eq!(
        paths_naming_stream(dir.path()),
        Vec::<std::path::PathBuf>::new()
    );
}

/// **A durable stream's log still opens**, registered or not.
#[tokio::test]
async fn a_durable_streams_log_still_opens() {
    let (broker, _dir) = broker_with_stream(true).await;
    let kind = felix_broker::LogKind::Stream;
    assert!(
        broker
            .shard_log(kind, TENANT, NAMESPACE, STREAM, 0)
            .await
            .is_some()
    );
    assert!(
        broker
            .shard_log(kind, TENANT, NAMESPACE, "unregistered", 0)
            .await
            .is_some()
    );
}

/// **A pass over an in-memory shard ships nothing and creates no log, and
/// still reports every follower level.** Placement moves an in-memory shard
/// on these reports, and there is nothing to copy for its records: they never
/// reach a log.
#[tokio::test]
async fn an_in_memory_shard_reports_without_a_log() {
    let (broker, dir) = broker_with_stream(false).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let follower = AcceptingFollower::default();

    let pass = pass(&follower, &broker, &router, &Unfenced).await;

    assert_eq!(
        paths_naming_stream(dir.path()),
        Vec::<std::path::PathBuf>::new()
    );
    assert_eq!(follower.batches(), Vec::new());
    assert_eq!(pass.reports.len(), 1, "{:?}", pass.reports);
    let report = &pass.reports[0];
    assert_eq!(report.key, key());
    assert_eq!(report.generation, 4);
    assert_eq!(report.tail, 0);
    assert_eq!(
        report.caught_up,
        vec!["broker-b".to_string(), "broker-c".to_string()]
    );
    assert!(!report.drained);
}

/// **A draining in-memory shard reports drained once its fence is quiet**, so
/// the cut-over of its move is not held on a log it does not have.
#[tokio::test]
async fn a_draining_in_memory_shard_reports_drained() {
    struct Quiet;
    impl WriteFence for Quiet {
        fn quiesced(&self, _key: &crate::ShardKey) -> bool {
            true
        }
    }
    let (broker, dir) = broker_with_stream(false).await;
    let router = draining_router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();

    let pass = pass(&follower, &broker, &router, &Quiet).await;

    assert_eq!(
        paths_naming_stream(dir.path()),
        Vec::<std::path::PathBuf>::new()
    );
    assert_eq!(pass.reports.len(), 1, "{:?}", pass.reports);
    assert!(pass.reports[0].drained);
    assert_eq!(pass.reports[0].caught_up, vec!["broker-b".to_string()]);
}

/// **An in-memory shard with replicas drains with nothing on disk**, after a
/// publish. The publish never reaches a log, nothing ships, and the drained
/// report still counts every follower caught up, so placement can finish a
/// move of a live leader on it (`is_drained`, #3158).
#[tokio::test]
async fn an_in_memory_shard_with_replicas_drains_on_an_empty_log() {
    let (broker, dir) = broker_with_stream(false).await;
    broker
        .publish(TENANT, NAMESPACE, STREAM, Bytes::from("in memory"))
        .await
        .expect("publish");
    let router = draining_router(LOCAL, &["broker-b", "broker-c"], 4);
    let follower = AcceptingFollower::default();

    let pass = pass(&follower, &broker, &router, &Unfenced).await;

    let report = pass
        .reports
        .into_iter()
        .find(|report| report.drained)
        .expect("an in-memory shard should drain");
    assert_eq!(report.tail, 0, "an in-memory publish reached the log");
    let mut caught_up = report.caught_up.clone();
    caught_up.sort();
    assert_eq!(
        caught_up,
        vec!["broker-b".to_string(), "broker-c".to_string()]
    );
    assert_eq!(follower.batches(), Vec::new());
    assert_eq!(
        paths_naming_stream(dir.path()),
        Vec::<std::path::PathBuf>::new()
    );
}

/// **A move's destination is reported caught up at the move's generation.**
/// Placement fences a move only once the leader reports at the generation
/// that added the copy (`reported_generation`) and names it caught up
/// (`ready_to_fence`); a restore waits on the same report.
#[tokio::test]
async fn an_in_memory_shards_move_destination_is_reported_caught_up() {
    let (broker, dir) = broker_with_stream(false).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let mut cursors = HashMap::default();
    pass_with(&follower, &broker, &router, &Unfenced, &mut cursors).await;

    publish(&router, LOCAL, &["broker-b", "broker-c"], 5);
    let pass = pass_with(&follower, &broker, &router, &Unfenced, &mut cursors).await;

    assert_eq!(pass.reports.len(), 1, "{:?}", pass.reports);
    let report = &pass.reports[0];
    assert_eq!(report.generation, 5);
    assert!(
        report.caught_up.contains(&"broker-c".to_string()),
        "the destination is not caught up: {report:?}",
    );
    assert_eq!(follower.batches(), Vec::new());
    assert_eq!(
        paths_naming_stream(dir.path()),
        Vec::<std::path::PathBuf>::new()
    );
}

/// **A stream deleted in memory and created again durable has a log.** What
/// the broker refuses follows the registration: a delete clears it, and the
/// new registration sets it again.
#[tokio::test]
async fn a_stream_recreated_durable_opens_its_log() {
    let (broker, _dir) = broker_with_stream(false).await;
    assert!(broker.stream_in_memory(TENANT, NAMESPACE, STREAM));
    broker
        .remove_stream(TENANT, NAMESPACE, STREAM)
        .await
        .expect("remove");
    assert!(!broker.stream_in_memory(TENANT, NAMESPACE, STREAM));
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            felix_broker::StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect("stream");

    assert!(!broker.stream_in_memory(TENANT, NAMESPACE, STREAM));
    assert!(
        broker
            .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
            .await
            .is_some()
    );
}
