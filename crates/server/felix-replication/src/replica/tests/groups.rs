//! A follower's dead-letter and cursor logs: a cursor never outlives the
//! dead letters it may have passed records on, and a broker without
//! consumer groups answers their fence as empty logs.

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

async fn held_in(broker: &Broker, log: felix_broker::LogKind) -> Vec<String> {
    broker
        .shard_log(log, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .read_from(0, 1 << 20)
        .await
        .expect("read")
        .iter()
        .map(|record| String::from_utf8(record.payload.to_vec()).expect("utf8"))
        .collect()
}

/// A follower holding generation 4's record, a dead letter and the cursor
/// past its record.
async fn following(dir: &std::path::Path) -> Arc<Broker> {
    let broker = group_broker(dir);
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    for (log, value) in [
        (felix_broker::LogKind::Stream, "a"),
        (felix_broker::LogKind::GroupDeadLetters, "dead 1"),
        (felix_broker::LogKind::GroupCursors, "at 2"),
    ] {
        let answer = handler.apply(SENDER, batch(4, 0, &[value]), log).await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "{answer:?}"
        );
    }
    broker
}

/// **A divergent dead letter a newer leader replaces takes the cursors of
/// its generation with it**, before it goes. Left, the cursor past offset 1
/// would outlive the only record that offset was given up on, and this
/// follower, promoted, would neither redeliver it nor list it.
#[tokio::test]
async fn a_dropped_dead_letter_takes_its_generations_cursors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = following(dir.path()).await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5));

    let answer = handler
        .apply(
            SENDER,
            batch(5, 0, &["other"]),
            felix_broker::LogKind::GroupDeadLetters,
        )
        .await;

    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(
        held_in(&broker, felix_broker::LogKind::GroupDeadLetters).await,
        vec!["other"]
    );
    assert!(
        held_in(&broker, felix_broker::LogKind::GroupCursors)
            .await
            .is_empty()
    );
}

/// **So does a dead-letter log the leader has the follower discard.**
#[tokio::test]
async fn a_rebuilt_dead_letter_log_takes_the_cursors_with_it() {
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
    assert!(
        held_in(&broker, felix_broker::LogKind::GroupCursors)
            .await
            .is_empty()
    );
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
