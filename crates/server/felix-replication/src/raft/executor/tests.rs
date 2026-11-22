use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::GroupThreads;

#[test]
fn groups_go_to_the_thread_with_the_fewest_and_free_their_place() {
    let threads = GroupThreads::start(2).expect("threads");
    let first = threads.place();
    let second = threads.place();
    let third = threads.place();
    assert_eq!(threads.placed(), vec![2, 1]);
    drop(first);
    drop(third);
    assert_eq!(threads.placed(), vec![0, 1]);
    let fourth = threads.place();
    assert_eq!(threads.placed(), vec![1, 1]);
    drop((second, fourth));
}

#[test]
fn a_panic_in_one_group_leaves_the_others_on_its_thread_running() {
    let threads = GroupThreads::start(1).expect("threads");
    let (sick, well) = (threads.place(), threads.place());
    let beats = Arc::new(AtomicU64::new(0));
    {
        let beats = Arc::clone(&beats);
        well.handle().spawn(async move {
            loop {
                beats.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
    }
    let panicked = sick.handle().spawn(async { panic!("a group task fails") });
    let outcome = futures::executor::block_on(panicked);
    assert!(outcome.expect_err("it panicked").is_panic());
    let after = beats.load(Ordering::Relaxed);
    let deadline = Instant::now() + Duration::from_secs(10);
    while beats.load(Ordering::Relaxed) < after + 5 {
        assert!(
            Instant::now() < deadline,
            "the other group stopped with the panic"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    // A group placed after the panic still runs there.
    let later = threads.place();
    assert_eq!(
        futures::executor::block_on(later.handle().spawn(async { 7 })).ok(),
        Some(7)
    );
}

/// A group whose tasks are always ready, as a hot leader's are, gives the
/// thread up between polls: another group's timer still fires within a
/// bound. tokio's budget makes the channel loop yield.
#[test]
fn a_hot_group_does_not_starve_another_on_its_thread() {
    const TIMER: Duration = Duration::from_millis(2);
    const BOUND: Duration = Duration::from_millis(50);
    let threads = GroupThreads::start(1).expect("threads");
    let (hot, quiet) = (threads.place(), threads.place());
    let stop = Arc::new(AtomicBool::new(false));
    let spins = Arc::new(AtomicU64::new(0));
    {
        let (stop, spins) = (Arc::clone(&stop), Arc::clone(&spins));
        hot.handle().spawn(async move {
            let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
            send.send(()).expect("send");
            while receive.recv().await.is_some() && !stop.load(Ordering::Relaxed) {
                spins.fetch_add(1, Ordering::Relaxed);
                send.send(()).expect("send");
            }
        });
    }
    let late = quiet.handle().spawn(async move {
        let mut worst = Duration::ZERO;
        for _ in 0..50 {
            let asked = Instant::now();
            tokio::time::sleep(TIMER).await;
            worst = worst.max(asked.elapsed().saturating_sub(TIMER));
        }
        worst
    });
    let worst = futures::executor::block_on(late).expect("the quiet group's timer");
    stop.store(true, Ordering::Relaxed);
    assert!(
        spins.load(Ordering::Relaxed) > 1_000,
        "the hot group did not run"
    );
    assert!(worst < BOUND, "the quiet group's timer was {worst:?} late");
}

#[test]
fn dropping_the_threads_stops_them() {
    let threads = GroupThreads::start(1).expect("threads");
    let placed = threads.place();
    let handle = placed.handle().clone();
    drop(threads);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if futures::executor::block_on(handle.spawn(async {})).is_err() {
            break;
        }
        assert!(Instant::now() < deadline, "the group thread kept running");
        std::thread::sleep(Duration::from_millis(1));
    }
    drop(placed);
}
