//! One batch at a time: what a follower's answer does to its cursor.

use super::*;

/// The ordinary case: ship from where the follower is, and move the cursor to
/// where the follower says it got to.
#[tokio::test]
async fn a_stored_batch_advances_the_cursor_to_what_the_follower_reported() {
    let (log, _dir) = leader_log(&["a", "b", "c"]).await;
    let follower = ScriptedFollower::new([Ok(stored(3))]);
    let mut cursor = cursor(0);

    let progress = ship_once(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        BATCH_BYTES,
        &Rebuilds::disabled(),
    )
    .await;

    assert_eq!(progress, Progress::Stored { durable_offset: 3 });
    assert_eq!(cursor.next_offset, 3);
    assert!(!cursor.stalled);
    assert_eq!(follower.sent(), vec![(0, vec_of(&["a", "b", "c"]))]);
}

/// **The follower's number wins, not the leader's arithmetic.** The follower
/// did the writing, and a partially applied batch would otherwise leave the two
/// sides disagreeing about what is stored.
#[tokio::test]
async fn the_cursor_follows_the_follower_rather_than_the_batch_size() {
    let (log, _dir) = leader_log(&["a", "b", "c"]).await;
    // Sent three, but the follower reports holding only two.
    let follower = ScriptedFollower::new([Ok(stored(2))]);
    let mut cursor = cursor(0);

    ship_once(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        BATCH_BYTES,
        &Rebuilds::disabled(),
    )
    .await;

    assert_eq!(cursor.next_offset, 2, "the leader trusted its own count");
}

/// **A follower cannot confirm more than it was sent.** The leader compared
/// nothing past the batch, so a higher answer would resume past records neither
/// side has checked — which is how an orphaned record from a dead leader
/// survives at an offset the new leader is about to reuse (#406).
#[tokio::test]
async fn the_cursor_does_not_follow_a_follower_past_the_batch() {
    let (log, _dir) = leader_log(&["a", "b", "c", "d"]).await;
    // Sent two, and the follower claims to hold four.
    let follower = ScriptedFollower::new([Ok(stored(4)), Ok(stored(4))]);
    let mut cursor = cursor(0);

    ship_once(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        ONE_RECORD_BYTES,
        &Rebuilds::disabled(),
    )
    .await;

    assert_eq!(
        cursor.next_offset, 1,
        "the cursor took the follower's word for records it was never sent",
    );

    ship_once(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        ONE_RECORD_BYTES,
        &Rebuilds::disabled(),
    )
    .await;

    assert_eq!(
        follower.sent(),
        vec![(0, vec_of(&["a"])), (1, vec_of(&["b"]))],
        "records were skipped: the leader resumed past what it had compared",
    );
}

/// **A gap rewinds the cursor.** A follower that lost records, or was rebuilt,
/// names the offset it wants and the leader resumes there — no separate
/// negotiation, and nothing kept on disk.
#[tokio::test]
async fn a_gap_rewinds_the_cursor_and_the_next_batch_starts_there() {
    let (log, _dir) = leader_log(&["a", "b", "c", "d"]).await;
    let follower = ScriptedFollower::new([Ok(refused(ErrorCode::LogGap, 1)), Ok(stored(4))]);
    let mut cursor = cursor(3);

    let first = ship_once(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        BATCH_BYTES,
        &Rebuilds::disabled(),
    )
    .await;
    assert_eq!(first, Progress::Resume { offset: 1 });
    assert_eq!(cursor.next_offset, 1);

    ship_once(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        BATCH_BYTES,
        &Rebuilds::disabled(),
    )
    .await;

    assert_eq!(
        follower.sent(),
        vec![(3, vec_of(&["d"])), (1, vec_of(&["b", "c", "d"])),],
        "the resend did not start where the follower asked",
    );
}

/// **Divergence stops this follower.** Two logs that disagree about stored
/// bytes do not converge by retrying, and continuing would ship records on top
/// of a log that is already wrong.
#[tokio::test]
async fn divergence_halts_the_follower_and_nothing_more_is_sent() {
    let (log, _dir) = leader_log(&["a", "b"]).await;
    let follower = ScriptedFollower::new([Ok(refused(ErrorCode::LogConflict, 0))]);
    let mut cursor = cursor(0);

    let progress = ship_once(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        BATCH_BYTES,
        &Rebuilds::disabled(),
    )
    .await;
    assert_eq!(progress, Progress::Halted(Halt::Diverged));
    assert_eq!(cursor.halted, Some(Halt::Diverged));

    // A halted follower is not shipped to again, even when asked.
    let again = ship_once(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        BATCH_BYTES,
        &Rebuilds::disabled(),
    )
    .await;

    assert_eq!(again, Progress::Halted(Halt::Diverged));
    assert_eq!(follower.sent().len(), 1, "a halted follower was shipped to");
}

/// **A fenced leader stops shipping.** The follower knows a newer generation,
/// so this broker is not the leader and has no business sending anything.
#[tokio::test]
async fn being_fenced_halts_the_follower() {
    let (log, _dir) = leader_log(&["a"]).await;
    let follower = ScriptedFollower::new([Ok(refused(ErrorCode::FencedEpoch, 0))]);
    let mut cursor = cursor(0);

    let progress = ship_once(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        BATCH_BYTES,
        &Rebuilds::disabled(),
    )
    .await;

    assert_eq!(progress, Progress::Halted(Halt::Fenced));
    assert_eq!(cursor.halted, Some(Halt::Fenced));
}

/// A refusal that resolves on its own leaves the cursor where it was, so the
/// same batch goes again.
#[tokio::test]
async fn a_transient_refusal_leaves_the_cursor_alone() {
    let (log, _dir) = leader_log(&["a", "b"]).await;
    let follower = ScriptedFollower::new([Ok(refused(ErrorCode::StaleRoute, 0)), Ok(stored(2))]);
    let mut cursor = cursor(0);

    assert_eq!(
        ship_once(
            &follower,
            &log,
            &shard(),
            felix_broker::LogKind::Stream,
            &mut cursor,
            BATCH_BYTES,
            &Rebuilds::disabled(),
        )
        .await,
        Progress::Retry,
    );
    assert_eq!(
        cursor.next_offset, 0,
        "a transient refusal moved the cursor"
    );

    ship_once(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        BATCH_BYTES,
        &Rebuilds::disabled(),
    )
    .await;
    assert_eq!(
        follower.sent(),
        vec![(0, vec_of(&["a", "b"])), (0, vec_of(&["a", "b"]))],
    );
}

/// An unreachable follower is retried, and — importantly — is not mistaken for
/// one that refused. Nothing was stored, so the cursor stays put.
#[tokio::test]
async fn an_unreachable_follower_is_retried_without_moving_the_cursor() {
    let (log, _dir) = leader_log(&["a"]).await;
    let follower = ScriptedFollower::new([Err(PeerError::Timeout {
        node_id: "broker-b".to_string(),
        timeout: std::time::Duration::from_secs(5),
    })]);
    let mut cursor = cursor(0);

    assert_eq!(
        ship_once(
            &follower,
            &log,
            &shard(),
            felix_broker::LogKind::Stream,
            &mut cursor,
            BATCH_BYTES,
            &Rebuilds::disabled(),
        )
        .await,
        Progress::Retry,
    );
    assert_eq!(cursor.next_offset, 0);
    assert!(cursor.halted.is_none(), "a timeout halted replication");
    assert!(cursor.stalled, "not reached, so its position is not moving");
}

/// A follower level with the leader is not shipped an empty batch.
#[tokio::test]
async fn a_follower_that_is_level_is_not_shipped_to() {
    let (log, _dir) = leader_log(&["a", "b"]).await;
    let follower = ScriptedFollower::new([]);
    let mut cursor = cursor(2);

    let progress = ship_once(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        BATCH_BYTES,
        &Rebuilds::disabled(),
    )
    .await;

    assert_eq!(progress, Progress::UpToDate);
    assert!(follower.sent().is_empty(), "an empty batch was shipped");
}

/// **A batch is bounded, so a follower far behind costs one batch of memory
/// rather than the distance it is behind.**
#[tokio::test]
async fn a_batch_is_bounded_rather_than_the_whole_backlog() {
    let (log, _dir) = leader_log(&["a", "b", "c", "d", "e", "f"]).await;
    let follower = ScriptedFollower::new([Ok(stored(2))]);
    let mut cursor = cursor(0);

    // A budget smaller than the backlog's payload bytes, so the read has to
    // stop short of the tail.
    ship_once(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        2,
        &Rebuilds::disabled(),
    )
    .await;

    let (_, payloads) = follower.sent().into_iter().next().expect("a batch");
    assert!(
        !payloads.is_empty(),
        "a budget smaller than one record shipped nothing at all",
    );
    assert!(
        payloads.len() < 6,
        "the whole backlog was read into one batch: {payloads:?}",
    );
}

/// The batch carries the checksum the follower will recompute. If these ever
/// disagreed, every healthy batch would be reported as corrupt.
#[tokio::test]
async fn the_batch_carries_the_checksum_the_follower_will_verify() {
    let (log, _dir) = leader_log(&["a", "b"]).await;
    let follower = ScriptedFollower::new([Ok(stored(2))]);
    let mut cursor = cursor(0);

    ship_once(
        &follower,
        &log,
        &shard(),
        felix_broker::LogKind::Stream,
        &mut cursor,
        BATCH_BYTES,
        &Rebuilds::disabled(),
    )
    .await;

    let batch = follower.sent.lock().expect("lock")[0].clone();
    assert_eq!(batch.checksum, batch_checksum(&batch.payloads, &[]));
    assert_eq!(
        batch.shard.generation, GENERATION,
        "the epoch was not carried"
    );
}
