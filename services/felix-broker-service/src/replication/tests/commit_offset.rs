//! The leader's commit offset, on its way to a follower.

use felix_wire::internal::ForwardPublishError;

use super::*;

fn commits(follower: &ScriptedFollower) -> Vec<Option<u64>> {
    follower
        .sent
        .lock()
        .expect("lock")
        .iter()
        .map(|batch| batch.commit_offset)
        .collect()
}

/// The offset rides on the batch, so a follower learns it from the records
/// themselves rather than from a message that can arrive out of order.
#[tokio::test]
async fn the_commit_offset_travels_with_the_batch() {
    let (log, _dir) = leader_log(&["a", "b", "c"]).await;
    let follower = ScriptedFollower::new([Ok(stored(3))]);
    let mut cursor = cursor(0);

    ship_once_with(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        BATCH_BYTES,
        &Rebuilds::disabled(),
        Some(2),
    )
    .await;

    assert_eq!(commits(&follower), vec![Some(2)]);
}

/// A follower from before commit offsets refuses the kind that carries one.
/// It is sent the same records as it reads them, and the old kinds from then
/// on, rather than being halted over a field it cannot read.
#[tokio::test]
async fn a_follower_that_predates_commit_offsets_is_sent_the_old_kind() {
    let (log, _dir) = leader_log(&["a", "b", "c"]).await;
    let follower = ScriptedFollower::new([
        Ok(InternalMessage::ForwardPublishError(ForwardPublishError {
            correlation_id: 0,
            code: ErrorCode::UnsupportedKind,
            detail: "this broker does not know frame kind 26".to_string(),
        })),
        Ok(stored(1)),
        Ok(stored(2)),
    ]);
    let mut cursor = cursor(0);

    let progress = ship_once_with(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        ONE_RECORD_BYTES,
        &Rebuilds::disabled(),
        Some(2),
    )
    .await;
    assert_eq!(progress, Progress::Stored { durable_offset: 1 });
    assert!(cursor.legacy_frames);

    ship_once_with(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        ONE_RECORD_BYTES,
        &Rebuilds::disabled(),
        Some(2),
    )
    .await;

    assert_eq!(commits(&follower), vec![Some(2), None, None]);
    assert_eq!(
        follower.sent(),
        vec![
            (0, vec_of(&["a"])),
            (0, vec_of(&["a"])),
            (1, vec_of(&["b"]))
        ]
    );
}
