//! Rebuilding a halted follower: discarding its copy and shipping from the
//! leader's base, under the policy's cap and rate.

use super::*;

fn policy(max_concurrent: usize) -> Rebuilds {
    Rebuilds::new(RebuildPolicy {
        max_concurrent,
        bytes_per_sec: 0,
    })
}

fn diverged(next_offset: u64) -> FollowerCursor {
    let mut cursor = cursor(next_offset);
    cursor.halted = Some(Halt::Diverged);
    cursor
}

async fn ship(
    follower: &ScriptedFollower,
    log: &StreamLog,
    cursor: &mut FollowerCursor,
    rebuilds: &Rebuilds,
) -> Progress {
    ship_once(
        follower,
        log,
        &shard(),
        felix_broker::LogKind::Stream,
        cursor,
        BATCH_BYTES,
        rebuilds,
    )
    .await
}

/// **A diverged follower is told to start again**, and once it has, the
/// halt is over: it is shipped from the leader's base like any follower
/// that far behind, and counted as level once it gets there.
#[tokio::test]
async fn a_diverged_follower_is_rebuilt_from_the_leaders_base() {
    let (log, _dir) = leader_log(&["a", "b", "c"]).await;
    let follower = ScriptedFollower::new([Ok(stored(0)), Ok(stored(3))]);
    let mut cursor = diverged(2);
    let rebuilds = policy(1);

    let first = ship(&follower, &log, &mut cursor, &rebuilds).await;

    assert_eq!(follower.rebuilds(), vec![0]);
    assert_eq!(cursor.halted, None);
    assert!(cursor.rebuilding);
    assert_eq!(
        first,
        Progress::Stored { durable_offset: 3 },
        "the first batch goes in the same pass as the rebuild"
    );
    assert_eq!(follower.sent(), vec![(0, vec_of(&["a", "b", "c"]))]);

    let level = ship(&follower, &log, &mut cursor, &rebuilds).await;
    assert_eq!(level, Progress::UpToDate);
    assert!(!cursor.rebuilding, "reaching the tail ends the rebuild");
    assert_eq!(
        caught_up(3, std::slice::from_ref(&cursor)),
        vec!["broker-b"]
    );
}

/// **The base is the leader's, not zero.** A leader that has trimmed its
/// history rebuilds the follower from what it still holds.
#[tokio::test]
async fn a_trimmed_leader_rebuilds_from_its_own_base() {
    let (log, base, _dir) = trimmed_history::trimmed_leader().await;
    let follower = ScriptedFollower::new([Ok(stored(base))]);
    let mut cursor = diverged(0);

    ship(&follower, &log, &mut cursor, &policy(1)).await;

    assert_eq!(follower.rebuilds(), vec![base]);
    assert!(cursor.rebuilding);
    assert_eq!(
        follower.sent().first().map(|(offset, _)| *offset),
        Some(base),
        "shipping resumed at the base the follower was rebuilt to"
    );
}

/// **A follower halted for needing a bootstrap it refused is rebuilt too.**
/// Its records and the leader's do not meet, and the leader's are the
/// ones the majority holds.
#[tokio::test]
async fn a_follower_that_refused_a_bootstrap_is_rebuilt() {
    let (log, _dir) = leader_log(&["a"]).await;
    let follower = ScriptedFollower::new([Ok(stored(0))]);
    let mut cursor = cursor(0);
    cursor.halted = Some(Halt::NeedsBootstrap);

    ship(&follower, &log, &mut cursor, &policy(1)).await;

    assert_eq!(follower.rebuilds(), vec![0]);
    assert_eq!(cursor.halted, None);
}

/// **With the policy off, a halt is an operator's.** Nothing is sent to
/// the follower and the halt stands, exactly as before rebuilds existed.
#[tokio::test]
async fn no_slots_means_the_halt_stands() {
    let (log, _dir) = leader_log(&["a"]).await;
    let follower = ScriptedFollower::new([]);
    let mut cursor = diverged(0);

    let progress = ship(&follower, &log, &mut cursor, &policy(0)).await;

    assert_eq!(progress, Progress::Halted(Halt::Diverged));
    assert_eq!(cursor.halted, Some(Halt::Diverged));
    assert!(follower.rebuilds().is_empty(), "a rebuild was asked for");
}

/// **A fenced leader rebuilds nobody.** It is the one in the wrong, and a
/// follower that discarded its copy on its say-so could lose records the
/// real leader has.
#[tokio::test]
async fn a_fenced_leader_does_not_rebuild() {
    let (log, _dir) = leader_log(&["a"]).await;
    let follower = ScriptedFollower::new([]);
    let mut cursor = cursor(0);
    cursor.halted = Some(Halt::Fenced);

    let progress = ship(&follower, &log, &mut cursor, &policy(4)).await;

    assert_eq!(progress, Progress::Halted(Halt::Fenced));
    assert!(
        follower.rebuilds().is_empty(),
        "a fenced leader asked for a rebuild"
    );
}

/// **The cap is across followers.** With one slot, the second halted
/// follower waits until the first has reached the tail, however many
/// passes that takes.
#[tokio::test]
async fn the_cap_holds_the_second_follower_until_the_first_is_level() {
    let (log, _dir) = leader_log(&["a", "b"]).await;
    let rebuilds = policy(1);
    // The first rebuilds and is shipped its batch; nothing more is scripted
    // for it until it is asked whether it is level.
    let first = ScriptedFollower::new([Ok(stored(0)), Ok(stored(2))]);
    let second = ScriptedFollower::new([Ok(stored(0)), Ok(stored(2))]);
    let mut first_cursor = diverged(0);
    let mut second_cursor = diverged(0);

    ship(&first, &log, &mut first_cursor, &rebuilds).await;
    let held = ship(&second, &log, &mut second_cursor, &rebuilds).await;

    assert!(first_cursor.rebuilding);
    assert_eq!(held, Progress::Halted(Halt::Diverged));
    assert!(second.rebuilds().is_empty(), "the cap was not honoured");

    // The first reaches the tail and gives its slot back.
    assert_eq!(
        ship(&first, &log, &mut first_cursor, &rebuilds).await,
        Progress::UpToDate
    );
    ship(&second, &log, &mut second_cursor, &rebuilds).await;

    assert_eq!(second.rebuilds(), vec![0]);
    assert!(second_cursor.rebuilding);
}

/// **A refusal gives the slot back and the halt stands.** Whatever the
/// follower's reason, asking again every pass would be a loop, and the
/// slot is worth more to a follower that will take it.
#[tokio::test]
async fn a_refused_rebuild_keeps_the_halt_and_frees_the_slot() {
    let (log, _dir) = leader_log(&["a"]).await;
    let rebuilds = policy(1);
    let refusing = ScriptedFollower::new([Ok(refused(ErrorCode::Unauthorized, 0))]);
    let willing = ScriptedFollower::new([Ok(stored(0))]);
    let mut refusing_cursor = diverged(0);
    let mut willing_cursor = diverged(0);

    let progress = ship(&refusing, &log, &mut refusing_cursor, &rebuilds).await;
    assert_eq!(progress, Progress::Halted(Halt::Diverged));
    assert_eq!(refusing_cursor.halted, Some(Halt::Diverged));
    assert!(!refusing_cursor.rebuilding);

    // And it is not asked again: nothing about a refusal changes with the
    // next pass, and asking would be a warning per pass for nothing.
    ship(&refusing, &log, &mut refusing_cursor, &rebuilds).await;
    assert_eq!(
        refusing.rebuilds(),
        vec![0],
        "a refusing follower was asked again"
    );

    ship(&willing, &log, &mut willing_cursor, &rebuilds).await;
    assert_eq!(willing.rebuilds(), vec![0], "the refusal kept the slot");
}

/// **A peer that predates rebuilds stays halted.** It answers a kind it
/// does not know with a generic error, which must not be read as consent.
#[tokio::test]
async fn an_older_follower_is_left_halted() {
    let (log, _dir) = leader_log(&["a"]).await;
    let follower = ScriptedFollower::new([Ok(InternalMessage::ForwardPublishError(
        felix_wire::internal::ForwardPublishError {
            correlation_id: 0,
            code: ErrorCode::UnsupportedKind,
            detail: "unknown kind".to_string(),
        },
    ))]);
    let mut cursor = diverged(0);

    let progress = ship(&follower, &log, &mut cursor, &policy(1)).await;

    assert_eq!(progress, Progress::Halted(Halt::Diverged));
    assert!(
        follower.sent().is_empty(),
        "records were shipped to a halted follower"
    );
}

/// **An unreachable follower is asked again.** Not answering is this
/// moment rather than a decision, and the slot goes back meanwhile.
#[tokio::test]
async fn an_unreachable_follower_is_asked_again() {
    let (log, _dir) = leader_log(&["a"]).await;
    let rebuilds = policy(1);
    let follower = ScriptedFollower::new([
        Err(PeerError::Unavailable {
            node_id: "broker-b".to_string(),
            detail: "down".to_string(),
        }),
        Ok(stored(0)),
    ]);
    let mut cursor = diverged(0);

    let progress = ship(&follower, &log, &mut cursor, &rebuilds).await;
    assert_eq!(progress, Progress::Halted(Halt::Diverged));
    assert!(!cursor.rebuild_refused);

    ship(&follower, &log, &mut cursor, &rebuilds).await;
    assert_eq!(follower.rebuilds(), vec![0, 0]);
    assert!(cursor.rebuilding);
}

/// **The rate bounds a rebuilding follower and nobody else.** Two batches
/// of the same size take longer under a rate than without one, and only
/// while the cursor is rebuilding.
#[tokio::test]
async fn the_rate_slows_a_rebuilding_follower() {
    let (log, _dir) = leader_log(&["0123456789", "0123456789"]).await;
    // Twenty bytes at forty per second: half a second of pacing.
    let rebuilds = Rebuilds::new(RebuildPolicy {
        max_concurrent: 1,
        bytes_per_sec: 40,
    });
    let follower = ScriptedFollower::new([Ok(stored(0)), Ok(stored(2))]);
    let mut cursor = diverged(0);

    let started = std::time::Instant::now();
    ship(&follower, &log, &mut cursor, &rebuilds).await;
    let paced = started.elapsed();

    let ordinary = ScriptedFollower::new([Ok(stored(2))]);
    let mut ordinary_cursor = cursor_at_zero();
    let started = std::time::Instant::now();
    ship(&ordinary, &log, &mut ordinary_cursor, &rebuilds).await;
    let unpaced = started.elapsed();

    assert!(
        paced >= std::time::Duration::from_millis(400),
        "the rebuild was not paced: {paced:?}"
    );
    assert!(
        unpaced < std::time::Duration::from_millis(200),
        "an ordinary follower was paced: {unpaced:?}"
    );
}

fn cursor_at_zero() -> FollowerCursor {
    cursor(0)
}
