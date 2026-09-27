use std::sync::Arc;
use std::time::Duration;

use super::*;

#[tokio::test]
async fn the_first_offset_proceeds_immediately() {
    let sequencer = CommitSequencer::new(0);
    let turn = sequencer.reserve(0, 1);
    turn.wait().await.expect("turn");
    drop(turn);
    assert_eq!(sequencer.next_offset(), 1);
}

#[tokio::test]
async fn a_later_offset_waits_for_its_predecessor() {
    let sequencer = Arc::new(CommitSequencer::new(0));

    // Offset 5 cannot proceed while the sequence is still at 0.
    let waiter = {
        let sequencer = Arc::clone(&sequencer);
        tokio::spawn(async move {
            let turn = sequencer.reserve(5, 6);
            turn.wait().await.expect("turn");
            drop(turn);
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiter.is_finished(), "offset 5 ran out of turn");

    // Releasing the intervening range lets it through.
    {
        let turn = sequencer.reserve(0, 5);
        turn.wait().await.expect("turn");
    }
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("waiter should be released")
        .expect("join");
    assert_eq!(sequencer.next_offset(), 6);
}

#[tokio::test]
async fn turns_are_granted_in_offset_order_regardless_of_arrival_order() {
    let sequencer = Arc::new(CommitSequencer::new(0));
    let observed = Arc::new(parking_lot::Mutex::new(Vec::new()));

    // Spawn the highest offset first so arrival order is the reverse of
    // offset order — exactly the interleaving the fsync wait can produce.
    let mut tasks = Vec::new();
    for offset in (0..8u64).rev() {
        let sequencer = Arc::clone(&sequencer);
        let observed = Arc::clone(&observed);
        tasks.push(tokio::spawn(async move {
            let turn = sequencer.reserve(offset, offset + 1);
            turn.wait().await.expect("turn");
            observed.lock().push(offset);
            drop(turn);
        }));
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    for task in tasks {
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("no deadlock")
            .expect("join");
    }

    assert_eq!(*observed.lock(), (0..8).collect::<Vec<_>>());
}

/// **A crowd parks, then drains one wake-up at a time, in order.**
///
/// Waking one waiter instead of all of them can fail two ways: wake the
/// wrong one and the order breaks, wake nobody and the drain stops dead.
///
/// Holding offset 0 is what makes the crowd real. Left alone the tasks run
/// in spawn order and each finds its turn already current, so nothing parks
/// and the test passes against any wake-up at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_crowd_drains_in_offset_order() {
    const WAITERS: u64 = 64;
    let sequencer = Arc::new(CommitSequencer::new(0));
    let observed = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let head = sequencer.reserve(0, 1);
    head.wait().await.expect("turn");

    let mut tasks = Vec::new();
    for offset in 1..=WAITERS {
        let sequencer = Arc::clone(&sequencer);
        let observed = Arc::clone(&observed);
        tasks.push(tokio::spawn(async move {
            let turn = sequencer.reserve(offset, offset + 1);
            turn.wait().await.expect("turn");
            observed.lock().push(offset);
            drop(turn);
        }));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        observed.lock().is_empty(),
        "a turn was granted while offset 0 was still held"
    );

    drop(head);

    for task in tasks {
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the drain stopped: a waiter was never woken")
            .expect("join");
    }
    assert_eq!(*observed.lock(), (1..=WAITERS).collect::<Vec<_>>());
    assert_eq!(sequencer.next_offset(), WAITERS + 1);
}

/// The one case that still needs to wake everybody: a reset moves `next`
/// somewhere unrelated, so waiters parked on offsets it discarded are not
/// next in any order and would otherwise wait forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reset_releases_a_whole_crowd() {
    let sequencer = Arc::new(CommitSequencer::new(0));
    let mut tasks = Vec::new();
    // Every one of these is behind offset 0, which nobody holds, so all of
    // them park.
    for offset in 1..32u64 {
        let sequencer = Arc::clone(&sequencer);
        tasks.push(tokio::spawn(async move {
            let turn = sequencer.reserve(offset, offset + 1);
            assert_eq!(turn.wait().await, Err(Superseded));
        }));
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    sequencer.reset(1000);

    for task in tasks {
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("a waiter was left parked behind a reset")
            .expect("join");
    }
}

#[tokio::test]
async fn a_dropped_turn_never_strands_the_stream() {
    let sequencer = Arc::new(CommitSequencer::new(0));

    // A publisher that takes its turn and then fails still releases it.
    let result: std::result::Result<(), ()> = async {
        let turn = sequencer.reserve(0, 1);
        turn.wait().await.expect("turn");
        Err(())
    }
    .await;
    assert!(result.is_err());

    // The next publisher is not blocked by the failure.
    let next = sequencer.reserve(1, 2);
    tokio::time::timeout(Duration::from_secs(5), next.wait())
        .await
        .expect("must not stall")
        .expect("turn");
}

#[tokio::test]
async fn an_abandoned_range_releases_the_ranges_behind_it() {
    let sequencer = Arc::new(CommitSequencer::new(0));

    // A publisher claims offsets 0..3 and is then cancelled before it ever
    // gets its turn — the shape of a client disconnecting during the fsync
    // wait, after its records are already on disk holding those offsets.
    {
        let _abandoned = sequencer.reserve(0, 3);
    }

    // The next range must still be reachable. Before the claim moved to
    // assignment time, an abandoned range released nothing and every later
    // publish on the stream waited forever.
    let next = sequencer.reserve(3, 4);
    tokio::time::timeout(Duration::from_secs(5), next.wait())
        .await
        .expect("an abandoned range stranded the stream")
        .expect("turn");
    assert_eq!(sequencer.next_offset(), 3);
}

#[tokio::test]
async fn a_range_cancelled_mid_wait_releases_too() {
    let sequencer = Arc::new(CommitSequencer::new(0));
    let blocker = sequencer.reserve(0, 5);

    // Waits for a turn that has not arrived, then is cancelled.
    let waiter = {
        let sequencer = Arc::clone(&sequencer);
        tokio::spawn(async move {
            let turn = sequencer.reserve(5, 9);
            turn.wait().await.expect("turn");
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    waiter.abort();
    let _ = waiter.await;
    drop(blocker);

    // The cancelled range released on drop, so the one behind it proceeds.
    let after = sequencer.reserve(9, 10);
    tokio::time::timeout(Duration::from_secs(5), after.wait())
        .await
        .expect("a cancelled waiter stranded the stream")
        .expect("turn");
}

#[tokio::test]
async fn a_cancelled_range_does_not_let_later_ranges_overtake_an_unfinished_one() {
    let sequencer = Arc::new(CommitSequencer::new(0));

    // A is in flight and has not applied yet.
    let a = sequencer.reserve(0, 3);
    // B reserves behind it, then is cancelled while waiting.
    {
        let _b = sequencer.reserve(3, 6);
    }

    // C must still wait: A has not applied, so nothing at or after offset 3
    // may reach the replay ring yet. If the cancelled B advanced the
    // sequence straight to 6, C would overtake A and the disk order would
    // disagree with the cursor order.
    let c = sequencer.reserve(6, 9);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), c.wait())
            .await
            .is_err(),
        "offset 6 overtook an unfinished range at offset 0"
    );

    // Once A applies, the contiguous prefix resolves and C proceeds.
    drop(a);
    tokio::time::timeout(Duration::from_secs(5), c.wait())
        .await
        .expect("C should be released once A completes")
        .expect("turn");
}

#[tokio::test]
async fn a_reset_releases_waiters_stuck_behind_a_vanished_offset() {
    let sequencer = Arc::new(CommitSequencer::new(10));

    // After a truncation the tail moves backwards; a publisher waiting on
    // an offset that no longer exists must be released rather than hang.
    let waiter = {
        let sequencer = Arc::clone(&sequencer);
        tokio::spawn(async move {
            let turn = sequencer.reserve(20, 21);
            turn.wait().await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiter.is_finished());

    sequencer.reset(20);
    let waited = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("reset should release the waiter")
        .expect("join");
    assert_eq!(waited, Err(Superseded), "a reset must not grant the turn");
}

/// A turn taken before a reset must not be granted after it: its offsets
/// describe a log the reset replaced, and applying them would put stale
/// records in front of readers.
#[tokio::test]
async fn a_turn_from_before_a_reset_is_superseded() {
    let sequencer = CommitSequencer::new(0);
    let turn = sequencer.reserve(0, 1);
    assert!(turn.is_current());
    sequencer.reset(0);
    assert!(!turn.is_current());
    assert_eq!(turn.wait().await, Err(Superseded));
}

#[tokio::test]
async fn a_stale_release_cannot_undo_a_reset() {
    let sequencer = CommitSequencer::new(100);
    let turn = sequencer.reserve(100, 101);
    turn.wait().await.expect("turn");
    // A truncation rewinds the log while the turn is held.
    sequencer.reset(5);
    drop(turn);
    assert_eq!(
        sequencer.next_offset(),
        5,
        "stale release moved the sequence"
    );
}
