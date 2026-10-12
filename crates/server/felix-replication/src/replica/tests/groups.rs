//! A follower's dead-letter and cursor logs: a cursor whose dead letters the
//! follower's log no longer holds is not what it serves once promoted, and a
//! broker without consumer groups answers their fence as empty logs.

use felix_wire::internal::{Fence, FenceOk, ReplicaLog, ReplicateRebuild};

use super::*;

fn group_broker(dir: &std::path::Path) -> Arc<Broker> {
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_durable_storage(
                felix_broker::DurableStorage::open(dir.join("streams"), config.clone())
                    .expect("storage"),
            )
            .with_consumer_groups(
                Arc::new(
                    felix_broker::ConsumerGroups::open(dir.join("groups"), config.clone())
                        .expect("groups"),
                ),
                Arc::new(
                    felix_broker::DeadLetters::open(dir.join("dead-letters"), config)
                        .expect("dead letters"),
                ),
                std::time::Duration::from_secs(30),
                3,
            ),
    )
}

fn put(key: &str, value: &[u8]) -> Bytes {
    felix_storage::cache::CacheOp::Put {
        key: key.to_string(),
        value: Bytes::copy_from_slice(value),
        expires_at_millis: 0,
        version: None,
    }
    .encode()
}

fn records(generation: u64, payloads: &[Bytes]) -> ReplicateRecords {
    ReplicateRecords {
        checksum: felix_wire::internal::batch_checksum(payloads, &[], &[]),
        payloads: payloads.to_vec(),
        ..batch(generation, 0, &[])
    }
}

fn workers() -> felix_broker::GroupKey {
    felix_broker::GroupKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        group: "workers".to_string(),
    }
}

/// A follower holding generation 4's record, `workers`' dead letter for
/// offset 1, and its cursor past it.
async fn following(dir: &std::path::Path) -> Arc<Broker> {
    let broker = group_broker(dir);
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    let mut cursor = vec![2];
    for field in [2u64, 1, 4, 0] {
        cursor.extend_from_slice(&field.to_be_bytes());
    }
    for (log, payload) in [
        (felix_broker::LogKind::Stream, Bytes::from_static(b"a")),
        (
            felix_broker::LogKind::GroupDeadLetters,
            put("workers\u{1f}1", b""),
        ),
        (felix_broker::LogKind::GroupCursors, put("workers", &cursor)),
    ] {
        let answer = handler.apply(SENDER, records(4, &[payload]), log).await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "{answer:?}"
        );
    }
    assert_eq!(serves(&broker).await, Some(2));
    broker
}

/// Where the broker would serve `workers` from, were it promoted now.
async fn serves(broker: &Broker) -> Option<u64> {
    let reader = broker.group_reader().expect("groups");
    reader.reset_shard(TENANT, NAMESPACE, STREAM, 0).await;
    reader.committed(&workers()).await.expect("cursor")
}

/// **A dead letter a newer leader replaced leaves the cursor past it
/// unserved.** The follower drops its divergent dead letter for another
/// group's at the same offset. Its cursor log still says `workers` passed
/// offset 1, but promoted, it serves `workers` from where the group was
/// placed, and offset 1 is delivered again.
#[tokio::test]
async fn a_replaced_dead_letter_leaves_its_cursor_unserved() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = following(dir.path()).await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5));

    let answer = handler
        .apply(
            SENDER,
            records(5, &[put("others\u{1f}0", b"")]),
            felix_broker::LogKind::GroupDeadLetters,
        )
        .await;

    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(serves(&broker).await, Some(0));
}

/// **So does a dead-letter log the leader has the follower discard.**
#[tokio::test]
async fn a_rebuilt_dead_letter_log_leaves_its_cursor_unserved() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = following(dir.path()).await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));

    let answer = handler
        .rebuild(
            SENDER,
            ReplicateRebuild {
                correlation_id: 1,
                shard: batch(4, 0, &[]).shard,
                log: ReplicaLog::GroupDeadLetters,
                base_offset: 0,
            },
        )
        .await;

    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(serves(&broker).await, Some(0));
}

/// **A broker without consumer groups answers the group fence as empty
/// logs**: it holds no group state and can store none, so the new leader
/// counts it rather than waiting on a promise it has nowhere to keep. An
/// older leader is still refused.
#[tokio::test]
async fn without_consumer_groups_the_group_fence_answers_empty() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    handler
        .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    let fence = |generation, log| Fence {
        correlation_id: 9,
        shard: batch(generation, 0, &[]).shard,
        log,
    };
    let stream = handler.fence(SENDER, fence(5, ReplicaLog::Stream)).await;
    assert!(matches!(stream, InternalMessage::FenceOk(_)), "{stream:?}");

    for log in [ReplicaLog::GroupDeadLetters, ReplicaLog::GroupCursors] {
        assert_eq!(
            handler.fence(SENDER, fence(5, log)).await,
            InternalMessage::FenceOk(FenceOk {
                correlation_id: 9,
                log_end: 0,
                commit_offset: 0,
                last_generation: 0,
            })
        );
        assert_eq!(
            refusal(&handler.fence(SENDER, fence(4, log)).await).code,
            ErrorCode::FencedEpoch
        );
    }
}
