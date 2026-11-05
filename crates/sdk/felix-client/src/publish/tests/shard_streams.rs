//! Adaptive shard streams: a shard leaves the pool only on evidence of a
//! stall, moves only once drained, and comes back once calm.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::fake_worker;
use crate::publish::routing::Selected;
use crate::publish::shard_streams::{CALM_FIRST, HeadWatch, STALL_FLOOR, now_nanos};
use crate::publish::writer::PublishWorker;
use crate::publish::{OpenWorker, Publisher, PublisherInner};

/// An opener that counts its opens and hands out fake writers, failing
/// while `fail` is set.
fn opener(opens: &Arc<AtomicUsize>, fail: &Arc<std::sync::atomic::AtomicBool>) -> OpenWorker {
    let opens = Arc::clone(opens);
    let fail = Arc::clone(fail);
    Arc::new(move || {
        let opens = Arc::clone(&opens);
        let fail = Arc::clone(&fail);
        Box::pin(async move {
            anyhow::ensure!(!fail.load(Ordering::SeqCst), "open refused");
            opens.fetch_add(1, Ordering::SeqCst);
            Ok(fake_worker())
        })
    })
}

struct Fixture {
    publisher: Publisher,
    opens: Arc<AtomicUsize>,
    fail: Arc<std::sync::atomic::AtomicBool>,
}

fn fixture(cap: usize) -> Fixture {
    let opens = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let pool = Arc::new(vec![fake_worker(), fake_worker()]);
    let publisher = Publisher {
        inner: Arc::new(PublisherInner::with_shard_streams(
            pool,
            cap,
            opener(&opens, &fail),
        )),
    };
    Fixture {
        publisher,
        opens,
        fail,
    }
}

/// The writer a publish to `shard` of `stream` gets: its own, or a pool slot.
async fn route(publisher: &Publisher, stream: &str, shard: u32) -> Own {
    match publisher
        .route("t", "ns", stream, Some(b"k".as_slice()), Some(shard))
        .await
        .expect("route")
        .worker
    {
        Selected::Shard(worker) => Own::Shard(worker),
        Selected::Pooled(_) => Own::Pooled,
    }
}

enum Own {
    Shard(Arc<PublishWorker>),
    Pooled,
}

impl Own {
    fn shard(self) -> Arc<PublishWorker> {
        match self {
            Self::Shard(worker) => worker,
            Self::Pooled => panic!("routed to the pool"),
        }
    }
}

/// The pooled writer `shard` of `stream` rides, with its head made the
/// shard's and overdue.
fn stall_pooled(f: &Fixture, stream: &str, id: u64) {
    let pooled = f
        .publisher
        .select_worker("t", "ns", stream)
        .expect("pooled writer");
    pooled.head.set(Some(id), now_nanos());
}

async fn ticket_id(publisher: &Publisher, stream: &str, shard: u32) -> u64 {
    publisher
        .route("t", "ns", stream, Some(b"k".as_slice()), Some(shard))
        .await
        .expect("route")
        .ticket
        .as_ref()
        .expect("a ticket")
        .id()
}

fn own_count(f: &Fixture) -> usize {
    f.publisher
        .inner
        .shard_streams
        .as_ref()
        .expect("shard streams")
        .own_count()
}

#[tokio::test]
async fn healthy_shards_ride_the_pool() {
    let f = fixture(4);
    for shard in 0..48 {
        assert!(matches!(
            route(&f.publisher, "orders", shard).await,
            Own::Pooled
        ));
    }
    assert_eq!(f.opens.load(Ordering::SeqCst), 0);
}

/// A shard whose answer is overdue at its pooled stream's head moves, but only
/// once its publishes in flight there are answered.
#[tokio::test(start_paused = true)]
async fn an_overdue_shard_moves_once_drained() {
    let f = fixture(4);
    let held = f
        .publisher
        .route("t", "ns", "orders", Some(b"k".as_slice()), Some(1))
        .await
        .expect("route");
    let id = held.ticket.as_ref().expect("ticket").id();
    stall_pooled(&f, "orders", id);
    tokio::time::advance(STALL_FLOOR * 2).await;
    let publisher = f.publisher.clone();
    let moving = tokio::spawn(async move { route(&publisher, "orders", 1).await.shard() });
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(
        !moving.is_finished(),
        "moved with a publish still in flight"
    );
    // Another shard of the same pooled stream, with nothing in flight behind
    // the overdue head, is not held up by the move: it leaves at once.
    route(&f.publisher, "orders", 2).await.shard();
    drop(held);
    moving.await.expect("moved");
    assert_eq!(f.opens.load(Ordering::SeqCst), 2);
    assert_eq!(own_count(&f), 2);
}

/// Calm for long enough on its own stream, the shard rejoins the pool.
#[tokio::test(start_paused = true)]
async fn a_calm_shard_returns_to_the_pool() {
    let f = fixture(4);
    let id = ticket_id(&f.publisher, "orders", 1).await;
    stall_pooled(&f, "orders", id);
    tokio::time::advance(STALL_FLOOR * 2).await;
    route(&f.publisher, "orders", 1).await.shard();
    // Drained before the move, so the pool's head is no longer the shard's.
    f.publisher
        .select_worker("t", "ns", "orders")
        .expect("pooled writer")
        .head
        .set(None, 0);
    tokio::time::advance(CALM_FIRST / 2).await;
    route(&f.publisher, "orders", 1).await.shard();
    tokio::time::advance(CALM_FIRST).await;
    assert!(matches!(
        route(&f.publisher, "orders", 1).await,
        Own::Pooled
    ));
    assert_eq!(own_count(&f), 0);
}

/// A publish dropped while it waits on its shard's move leaves the shard
/// routable, not stuck moving.
#[tokio::test(start_paused = true)]
async fn a_move_dropped_midway_leaves_the_shard_routable() {
    let f = fixture(4);
    let held = f
        .publisher
        .route("t", "ns", "orders", Some(b"k".as_slice()), Some(1))
        .await
        .expect("route");
    let id = held.ticket.as_ref().expect("ticket").id();
    stall_pooled(&f, "orders", id);
    tokio::time::advance(STALL_FLOOR * 2).await;
    let dropped = tokio::time::timeout(Duration::from_millis(10), route(&f.publisher, "orders", 1));
    assert!(
        dropped.await.is_err(),
        "the move should wait for the held publish"
    );
    drop(held);
    tokio::time::timeout(Duration::from_secs(1), route(&f.publisher, "orders", 1))
        .await
        .expect("the shard was left moving");
    assert_eq!(own_count(&f), f.opens.load(Ordering::SeqCst));
}

/// A shard that stalls again soon after each return stays out longer each
/// time, so it moves a handful of times in two minutes, not every few seconds.
#[tokio::test(start_paused = true)]
async fn a_flapping_shard_does_not_churn() {
    let f = fixture(4);
    let id = ticket_id(&f.publisher, "orders", 1).await;
    let pooled = f
        .publisher
        .select_worker("t", "ns", "orders")
        .expect("pooled writer");
    for _ in 0..20 {
        // Stall wherever the shard is: the pool's head, or its own stream's.
        let own = match route(&f.publisher, "orders", 1).await {
            Own::Shard(own) => {
                own.head.set(Some(id), now_nanos());
                Some(own)
            }
            Own::Pooled => {
                stall_pooled(&f, "orders", id);
                None
            }
        };
        tokio::time::advance(STALL_FLOOR * 2).await;
        route(&f.publisher, "orders", 1).await;
        // The late answer lands.
        pooled.head.set(None, 0);
        if let Some(own) = own {
            own.head.set(None, 0);
        }
        tokio::time::advance(Duration::from_secs(6)).await;
        route(&f.publisher, "orders", 1).await;
    }
    let opens = f.opens.load(Ordering::SeqCst);
    assert!(opens <= 4, "{opens} moves in two minutes of flapping");
}

/// A shard that publishes without an ack is never moved: nothing would tell
/// it when those publishes reached the broker.
#[tokio::test(start_paused = true)]
async fn an_unacked_shard_stays_put() {
    let f = fixture(4);
    let routed = f
        .publisher
        .route("t", "ns", "orders", Some(b"k".as_slice()), Some(1))
        .await
        .expect("route");
    let ticket = routed.ticket.clone().expect("ticket");
    ticket.pin();
    let id = ticket.id();
    drop((ticket, routed));
    stall_pooled(&f, "orders", id);
    tokio::time::advance(STALL_FLOOR * 2).await;
    assert!(matches!(
        route(&f.publisher, "orders", 1).await,
        Own::Pooled
    ));
    assert_eq!(f.opens.load(Ordering::SeqCst), 0);
}

/// Past the cap, a stalled shard stays pooled.
#[tokio::test(start_paused = true)]
async fn past_the_cap_a_stalled_shard_stays_pooled() {
    let f = fixture(1);
    for shard in [1, 2] {
        let id = ticket_id(&f.publisher, "orders", shard).await;
        stall_pooled(&f, "orders", id);
        tokio::time::advance(STALL_FLOOR * 2).await;
        let own = route(&f.publisher, "orders", shard).await;
        assert_eq!(matches!(own, Own::Shard(_)), shard == 1);
    }
    assert_eq!(f.opens.load(Ordering::SeqCst), 1);
}

/// A stream that cannot be opened leaves the shard on the pool, and the
/// publish goes there.
#[tokio::test(start_paused = true)]
async fn a_failed_open_keeps_the_shard_pooled() {
    let f = fixture(4);
    f.fail.store(true, Ordering::SeqCst);
    let id = ticket_id(&f.publisher, "orders", 1).await;
    stall_pooled(&f, "orders", id);
    tokio::time::advance(STALL_FLOOR * 2).await;
    assert!(matches!(
        route(&f.publisher, "orders", 1).await,
        Own::Pooled
    ));
    assert_eq!(own_count(&f), 0);
}

/// Moves `shard` of "orders" onto its own stream and calms the pool's head.
async fn move_out(f: &Fixture, shard: u32) -> (u64, Arc<PublishWorker>) {
    let id = ticket_id(&f.publisher, "orders", shard).await;
    stall_pooled(f, "orders", id);
    tokio::time::advance(STALL_FLOOR * 2).await;
    let own = route(&f.publisher, "orders", shard).await.shard();
    f.publisher
        .select_worker("t", "ns", "orders")
        .expect("pooled writer")
        .head
        .set(None, 0);
    (id, own)
}

/// An unacked publish written while the shard drains for its way back pins
/// it: the shard stays on its own stream rather than send its next publish on
/// another stream that could overtake it.
#[tokio::test(start_paused = true)]
async fn a_pin_during_the_drain_back_keeps_the_shard_out() {
    let f = fixture(4);
    move_out(&f, 1).await;
    let held = f
        .publisher
        .route("t", "ns", "orders", Some(b"k".as_slice()), Some(1))
        .await
        .expect("route");
    assert!(matches!(held.worker, Selected::Shard(_)));
    tokio::time::advance(CALM_FIRST * 2).await;
    let publisher = f.publisher.clone();
    let back =
        tokio::spawn(async move { matches!(route(&publisher, "orders", 1).await, Own::Pooled) });
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(!back.is_finished(), "went back with a publish in flight");
    held.ticket.as_ref().expect("ticket").pin();
    drop(held);
    assert!(
        !back.await.expect("routed"),
        "a pinned shard went back to the pool"
    );
    assert_eq!(own_count(&f), 1);
}

/// The first answer seeds a stream's usual wait, so a broker that is slow for
/// every shard does not look stalled while the average warms up.
#[test]
fn the_first_answer_seeds_the_usual_wait() {
    let head = HeadWatch::default();
    head.sample(Duration::from_millis(300));
    assert!(head.bound() >= Duration::from_secs(1), "{:?}", head.bound());
}

/// On its own stream a shard is judged by that stream's usual wait, not the
/// floor: steady answers slower than the floor still let it go back.
#[tokio::test(start_paused = true)]
async fn a_steady_slow_own_stream_lets_the_shard_go_back() {
    let f = fixture(4);
    let (id, own) = move_out(&f, 1).await;
    own.head.sample(Duration::from_millis(300));
    for _ in 0..30 {
        own.head.set(Some(id), now_nanos());
        tokio::time::advance(Duration::from_millis(300)).await;
        if matches!(route(&f.publisher, "orders", 1).await, Own::Pooled) {
            return;
        }
        own.head.set(None, 0);
    }
    panic!("a shard answered steadily at 300 ms never went back");
}

/// A shard that stops publishing here, as when its leader moves to another
/// broker, gives its stream and its place under the cap back once idle, even
/// though nothing routes that shard again.
#[tokio::test(start_paused = true)]
async fn an_idle_own_stream_is_given_back() {
    let f = fixture(4);
    move_out(&f, 1).await;
    assert_eq!(own_count(&f), 1);
    tokio::time::advance(Duration::from_secs(40)).await;
    assert!(matches!(
        route(&f.publisher, "orders", 2).await,
        Own::Pooled
    ));
    assert_eq!(own_count(&f), 0);
}

/// Places for shards nothing publishes to any more are forgotten once there
/// are many, so a client publishing to many streams does not grow for good.
#[tokio::test(start_paused = true)]
async fn unused_places_are_forgotten() {
    let f = fixture(4);
    for shard in 0..5000 {
        route(&f.publisher, "orders", shard).await;
    }
    tokio::time::advance(CALM_FIRST * 2).await;
    route(&f.publisher, "orders", 0).await;
    let shard_streams = f
        .publisher
        .inner
        .shard_streams
        .as_ref()
        .expect("shard streams");
    assert!(
        shard_streams.cell_count() < 100,
        "{} places kept",
        shard_streams.cell_count()
    );
}

/// A stream that finishes opening after `close` is not installed: close has
/// already collected the writers to finish, so it would never be finished.
#[tokio::test(start_paused = true)]
async fn a_stream_opened_after_close_is_dropped() {
    let opens = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Notify::new());
    let open: OpenWorker = {
        let opens = Arc::clone(&opens);
        let gate = Arc::clone(&gate);
        Arc::new(move || {
            let opens = Arc::clone(&opens);
            let gate = Arc::clone(&gate);
            Box::pin(async move {
                gate.notified().await;
                opens.fetch_add(1, Ordering::SeqCst);
                Ok(fake_worker())
            })
        })
    };
    let publisher = Publisher {
        inner: Arc::new(PublisherInner::with_shard_streams(
            Arc::new(vec![fake_worker()]),
            4,
            open,
        )),
    };
    let f = Fixture {
        publisher,
        opens,
        fail: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    };
    let id = ticket_id(&f.publisher, "orders", 1).await;
    stall_pooled(&f, "orders", id);
    tokio::time::advance(STALL_FLOOR * 2).await;
    let publisher = f.publisher.clone();
    let moving =
        tokio::spawn(async move { matches!(route(&publisher, "orders", 1).await, Own::Pooled) });
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    let shard_streams = f
        .publisher
        .inner
        .shard_streams
        .as_ref()
        .expect("shard streams");
    assert!(shard_streams.close().is_empty());
    gate.notify_waiters();
    assert!(
        moving.await.expect("routed"),
        "a stream opened after close was used"
    );
    assert_eq!(f.opens.load(Ordering::SeqCst), 1);
    assert_eq!(own_count(&f), 0);
}
