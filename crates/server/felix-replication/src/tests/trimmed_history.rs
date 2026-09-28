//! A follower asking for records the leader has already trimmed.
//!
//! Shipping cannot bridge this: the records are gone from the leader too. The
//! leader offers the follower the one fact it cannot work out for itself —
//! where the surviving log begins — and the follower decides whether it can
//! take it.

use super::*;

/// A leader whose retention has already removed the start of its log.
pub(super) async fn trimmed_leader() -> (StreamLog, u64, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            // Tiny segments and a tight bound, so a handful of records
            // forces the trim a long-lived stream would reach in time.
            segment_size_bytes: 128,
            index_spacing_bytes: 64,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            retention_bytes: Some(256),
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let log = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open");
    for i in 0..40 {
        log.append(&[Bytes::from(format!("value-{i:03}"))])
            .await
            .expect("append");
    }
    log.enforce_retention_now().await.expect("retention");
    let base = log.base_offset();
    assert!(
        base > 0,
        "retention did not trim, so the case is not set up"
    );
    (log, base, dir)
}

/// What the follower was asked, rather than sent.
fn offers(follower: &ScriptedFollower) -> Vec<u64> {
    follower.offered()
}

/// **The leader offers a bootstrap rather than shipping a range it does not
/// have.** The offer names the oldest offset this leader still holds.
#[tokio::test]
async fn a_follower_below_the_leaders_base_is_offered_a_bootstrap() {
    let (log, base, _dir) = trimmed_leader().await;
    let follower = ScriptedFollower::new([Ok(stored(base))]);
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

    assert_eq!(progress, Progress::Resume { offset: base });
    assert_eq!(offers(&follower), vec![base]);
    assert!(
        follower.sent().is_empty(),
        "records were shipped from a range the leader no longer holds",
    );
}

/// Once the follower has taken the offer, the cursor sits at the surviving
/// base and ordinary shipping resumes from there.
#[tokio::test]
async fn an_accepted_bootstrap_resumes_ordinary_shipping() {
    let (log, base, _dir) = trimmed_leader().await;
    let tail = log.tail_offset().await.expect("tail");
    let follower = ScriptedFollower::new([Ok(stored(base)), Ok(stored(tail))]);
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
    assert_eq!(cursor.next_offset, base);

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

    assert!(matches!(progress, Progress::Stored { .. }), "{progress:?}");
    let (first, _) = follower.sent().into_iter().next().expect("a batch");
    assert_eq!(first, base, "shipping did not resume at the surviving base");
}

/// **A follower that refuses the offer is halted.** It holds records of its
/// own, so a person has to decide what becomes of them; asking again would
/// never change the answer.
#[tokio::test]
async fn a_refused_bootstrap_halts_the_follower() {
    let (log, _base, _dir) = trimmed_leader().await;
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

    assert_eq!(progress, Progress::Halted(Halt::NeedsBootstrap));
    assert_eq!(cursor.halted, Some(Halt::NeedsBootstrap));
}

/// And it stays halted: a refusal is not retried on the next pass.
#[tokio::test]
async fn a_halted_follower_is_not_offered_again() {
    let (log, _base, _dir) = trimmed_leader().await;
    let follower = ScriptedFollower::new([Ok(refused(ErrorCode::LogConflict, 0))]);
    let mut cursor = cursor(0);

    for _ in 0..3 {
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
    }

    assert_eq!(offers(&follower).len(), 1, "a refused offer was repeated");
}

/// An unreachable follower keeps its offer open: nothing about the network
/// says the follower would refuse.
#[tokio::test]
async fn an_unreachable_follower_keeps_its_offer_open() {
    let (log, _base, _dir) = trimmed_leader().await;
    let follower = ScriptedFollower::new([Err(PeerError::Timeout {
        node_id: "broker-b".to_string(),
        timeout: std::time::Duration::from_secs(5),
    })]);
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

    assert_eq!(progress, Progress::Retry);
    assert!(cursor.halted.is_none(), "a timeout halted replication");
}

/// **A follower still being offered a bootstrap counts toward no quorum.**
/// It is not merely behind; it cannot arrive until it accepts.
#[tokio::test]
async fn a_follower_that_refused_counts_toward_no_quorum() {
    let (log, _base, _dir) = trimmed_leader().await;
    let follower = ScriptedFollower::new([Ok(refused(ErrorCode::LogConflict, 0))]);
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

    let tail = log.tail_offset().await.expect("tail");
    assert_eq!(quorum_offset(tail, &[cursor.clone(), super::cursor(0)]), 0);
}

/// A follower at or above the leader's base is ordinary catch-up, not a
/// bootstrap. The line is exactly the base offset.
#[tokio::test]
async fn a_follower_at_the_base_is_ordinary_catch_up() {
    let (log, base, _dir) = trimmed_leader().await;
    let follower = ScriptedFollower::new([Ok(stored(base + 1))]);
    let mut cursor = cursor(base);

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

    assert!(
        matches!(progress, Progress::Stored { .. }),
        "a follower at the surviving base was offered a bootstrap: {progress:?}",
    );
    assert!(offers(&follower).is_empty());
}
