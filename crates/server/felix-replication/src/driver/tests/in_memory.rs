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
    replicate_once_with(
        follower,
        broker,
        router,
        fence,
        &crate::promotion::NoGate,
        &QuorumMarks::new(),
        None,
        &mut HashMap::default(),
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
