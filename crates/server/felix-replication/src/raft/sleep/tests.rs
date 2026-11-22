use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};
use std::time::Duration;

use openraft::rt::{AsyncRuntime, Instant};
use openraft_rt_tokio::TokioInstant;

use super::super::draws::ShardRuntime;
use super::ShardSleep;

#[derive(Default)]
struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn a_close_deadline_yields_and_arms_no_timer() {
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);
    let deadline = TokioInstant::now() + Duration::from_micros(1);
    let mut sleep = ShardSleep::until(deadline);
    assert!(Pin::new(&mut sleep).poll(&mut cx).is_pending());
    assert_eq!(
        wakes.0.load(Ordering::Relaxed),
        1,
        "woken to be polled again"
    );
    assert!(matches!(sleep, ShardSleep::Soon { .. }));
    while TokioInstant::now() < deadline {
        std::hint::spin_loop();
    }
    assert!(Pin::new(&mut sleep).poll(&mut cx).is_ready());
    assert!(
        matches!(sleep, ShardSleep::Soon { .. }),
        "no timer was armed"
    );
}

#[test]
fn a_nanosecond_sleep_still_yields() {
    let waker = Waker::from(Arc::new(Wakes::default()));
    let mut cx = Context::from_waker(&waker);
    let deadline = TokioInstant::now() + Duration::from_nanos(1);
    let mut sleep = ShardRuntime::sleep_until(deadline);
    assert!(Pin::new(&mut sleep).poll(&mut cx).is_pending());
    while TokioInstant::now() <= deadline {
        std::hint::spin_loop();
    }
    assert!(Pin::new(&mut sleep).poll(&mut cx).is_ready());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn microsecond_sleeps_do_not_wait_for_timer_ticks() {
    // Each would last a whole tick on tokio's timer: 50 ms or more.
    let start = TokioInstant::now();
    for _ in 0..50 {
        ShardRuntime::sleep(Duration::from_micros(1)).await;
    }
    let took = TokioInstant::now() - start;
    assert!(took < Duration::from_millis(25), "50 sleeps took {took:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_sleep_ends_before_its_deadline() {
    for wait in [
        Duration::from_micros(1),
        Duration::from_micros(300),
        Duration::from_micros(999),
        Duration::from_millis(1),
        Duration::from_millis(5),
    ] {
        let start = TokioInstant::now();
        ShardRuntime::sleep(wait).await;
        assert!(TokioInstant::now() - start >= wait, "{wait:?}");
        let deadline = TokioInstant::now() + wait;
        ShardRuntime::sleep_until(deadline).await;
        assert!(TokioInstant::now() >= deadline, "{wait:?}");
    }
}

#[tokio::test]
async fn a_tick_or_longer_is_a_timer() {
    let sleep = ShardSleep::until(TokioInstant::now() + Duration::from_millis(2));
    assert!(matches!(sleep, ShardSleep::Timer(_)));
}

#[test]
fn a_close_deadline_still_early_after_its_yields_arms_the_timer() {
    let waker = Waker::from(Arc::new(Wakes::default()));
    let mut cx = Context::from_waker(&waker);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime");
    let _entered = runtime.enter();
    let deadline = TokioInstant::now() + Duration::from_micros(900);
    let mut sleep = ShardSleep::until(deadline);
    while matches!(sleep, ShardSleep::Soon { .. }) {
        // A preempted thread can pass the deadline: Ready is then right.
        if Pin::new(&mut sleep).poll(&mut cx).is_ready() {
            assert!(TokioInstant::now() >= deadline);
            return;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_close_deadline_still_ends_under_a_paused_clock() {
    // The paused clock moves only while every task waits on a timer.
    let start = TokioInstant::now();
    let sleep = pin!(ShardRuntime::sleep(Duration::from_micros(500)));
    tokio::time::timeout(Duration::from_secs(5), sleep)
        .await
        .expect("ended");
    assert!(TokioInstant::now() - start >= Duration::from_micros(500));
}
