use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::FutureExt;
use tokio::sync::watch;

use super::{Closed, Queue};

/// Lets every task the last advance woke run.
async fn settle() {
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn an_advance_wakes_only_the_waiters_it_reaches_in_key_order() {
    let (tx, rx) = watch::channel(0u64);
    let queue = Queue::new();
    let done = Rc::new(std::cell::RefCell::new(Vec::new()));
    let local = tokio::task::LocalSet::new();
    for key in [5u64, 1, 4, 2, 3] {
        let (rx, waiters, done) = (rx.clone(), queue.waiters(), Rc::clone(&done));
        local.spawn_local(async move {
            let at = waiters
                .wait_for(&rx, key, |v| (*v >= key).then_some(*v))
                .await;
            done.borrow_mut().push((key, at));
        });
    }
    local
        .run_until(async {
            settle().await;
            tx.send_replace(3);
            queue.advance(Some(3));
            settle().await;
            assert_eq!(*done.borrow(), vec![(1, Ok(3)), (2, Ok(3)), (3, Ok(3))]);
            tx.send_replace(5);
            queue.advance(Some(5));
            settle().await;
        })
        .await;
    assert_eq!(done.borrow()[3..], [(4, Ok(5)), (5, Ok(5))]);
    assert_eq!(queue.waiters().queued(), 0);
}

/// The send lands after the waiter looked and before it sleeps. A waiter
/// that looked before it queued would sleep through the advance.
#[tokio::test(flavor = "current_thread")]
async fn an_advance_between_the_look_and_the_sleep_is_not_lost() {
    let value = Arc::new(AtomicU64::new(0));
    let (_tx, rx) = watch::channel(Arc::clone(&value));
    let queue = Queue::new();
    let waiters = queue.waiters();
    let looks = Cell::new(0);
    let wait = waiters.wait_for(&rx, 7, |v| {
        looks.set(looks.get() + 1);
        let now = v.load(Ordering::SeqCst);
        if now >= 7 {
            return Some(now);
        }
        // The sender's send and advance, right after this look.
        v.store(7, Ordering::SeqCst);
        queue.advance(Some(7));
        None
    });
    let found = tokio::time::timeout(Duration::from_secs(5), wait).await;
    assert_eq!(found.expect("the waiter slept through the advance"), Ok(7));
    assert_eq!(looks.get(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn a_value_already_there_answers_without_an_advance() {
    let (_tx, rx) = watch::channel(9u64);
    let queue = Queue::new();
    let found = queue
        .waiters()
        .wait_for(&rx, 4, |v| (*v >= 4).then_some(*v))
        .now_or_never();
    assert_eq!(found, Some(Ok(9)));
}

/// Reached but not satisfied, as a written position in a later term is for
/// a waiter that names an earlier one: it waits for the next advance, and
/// does not spin.
#[tokio::test(flavor = "current_thread")]
async fn a_reached_key_that_does_not_satisfy_waits_for_the_next_advance() {
    let (tx, rx) = watch::channel(0u64);
    let queue = Queue::new();
    queue.advance(Some(10));
    let looks = Cell::new(0);
    let waiters = queue.waiters();
    let mut wait = std::pin::pin!(waiters.wait_for(&rx, 5, |v| {
        looks.set(looks.get() + 1);
        (*v == 12).then_some(*v)
    }));
    assert!((&mut wait).now_or_never().is_none());
    // The look before queueing, then the one after.
    assert_eq!(looks.get(), 2);
    tx.send_replace(11);
    queue.advance(Some(11));
    assert!((&mut wait).now_or_never().is_none());
    assert_eq!(looks.get(), 3);
    tx.send_replace(12);
    queue.advance(Some(12));
    assert_eq!((&mut wait).now_or_never(), Some(Ok(12)));
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_the_queue_wakes_every_waiter_closed() {
    let (tx, rx) = watch::channel(0u64);
    let queue = Queue::new();
    let waiters = queue.waiters();
    let mut wait = std::pin::pin!(waiters.wait_for(&rx, 3, |v| (*v >= 3).then_some(*v)));
    assert!((&mut wait).now_or_never().is_none());
    drop(queue);
    assert_eq!((&mut wait).now_or_never(), Some(Err(Closed)));
    // The value is still read first, as `watch` does on a closed channel.
    tx.send_replace(3);
    let found = waiters
        .wait_for(&rx, 3, |v| (*v >= 3).then_some(*v))
        .now_or_never();
    assert_eq!(found, Some(Ok(3)));
}

#[tokio::test(flavor = "current_thread")]
async fn a_dropped_wait_leaves_nothing_queued() {
    let (_tx, rx) = watch::channel(0u64);
    let queue = Queue::new();
    let waiters = queue.waiters();
    let mut wait = Box::pin(waiters.wait_for(&rx, 3, |v| (*v >= 3).then_some(*v)));
    assert!((&mut wait).now_or_never().is_none());
    assert_eq!(waiters.queued(), 1);
    drop(wait);
    assert_eq!(waiters.queued(), 0);
}

/// 512 waiters, each on its own index, and the value advanced one index at
/// a time: the shape of a commit wait under 512 writes in flight. Counts
/// how often a waiter looked at the value.
#[tokio::test(flavor = "current_thread")]
async fn an_advance_wakes_the_waiters_it_satisfies_not_the_whole_queue() {
    const WAITERS: u64 = 512;
    async fn looks(queued: bool) -> usize {
        let (tx, rx) = watch::channel(0u64);
        let queue = Queue::new();
        let looks = Arc::new(AtomicUsize::new(0));
        let local = tokio::task::LocalSet::new();
        for key in 1..=WAITERS {
            let (mut rx, waiters, looks) = (rx.clone(), queue.waiters(), Arc::clone(&looks));
            local.spawn_local(async move {
                let ready = |v: &u64| {
                    looks.fetch_add(1, Ordering::Relaxed);
                    (*v >= key).then_some(())
                };
                if queued {
                    waiters.wait_for(&rx, key, ready).await.unwrap();
                } else {
                    rx.wait_for(|v| ready(v).is_some()).await.unwrap();
                }
            });
        }
        local
            .run_until(async {
                settle().await;
                for at in 1..=WAITERS {
                    tx.send_replace(at);
                    queue.advance(Some(at));
                    settle().await;
                }
            })
            .await;
        local.await;
        looks.load(Ordering::Relaxed)
    }
    let watched = looks(false).await;
    let queued = looks(true).await;
    println!("{WAITERS} waiters: watch::wait_for looked {watched} times, the queue {queued}");
    // A look before queueing, one after, one once woken.
    assert_eq!(queued, 3 * WAITERS as usize);
    assert!(
        watched > 100 * WAITERS as usize,
        "watch looked {watched} times"
    );
}
