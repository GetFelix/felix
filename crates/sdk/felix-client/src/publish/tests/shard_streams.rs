//! Per-shard publish streams: which writer a shard gets, the cap, and what
//! happens when a shard's writer dies.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use felix_wire::AckMode;

use super::fake_worker;
use crate::publish::routing::Selected;
use crate::publish::writer::{PublishRequest, PublishWorker};
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
        .route("t", "ns", stream, Some(shard))
        .await
        .expect("route")
    {
        Selected::Shard(worker) => Own::Shard(worker),
        Selected::Pooled(worker) => Own::Pooled(worker as *const PublishWorker),
    }
}

enum Own {
    Shard(Arc<PublishWorker>),
    Pooled(*const PublishWorker),
}

impl Own {
    fn shard(self) -> Arc<PublishWorker> {
        match self {
            Self::Shard(worker) => worker,
            Self::Pooled(_) => panic!("routed to the pool"),
        }
    }
}

async fn kill(worker: &PublishWorker) {
    let (response, answered) = tokio::sync::oneshot::channel();
    worker
        .tx
        .send(PublishRequest::Finish { response })
        .await
        .expect("worker running");
    let _ = answered.await;
    while !worker.tx.is_closed() {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn a_shard_gets_a_stream_of_its_own() {
    let f = fixture(4);
    let own = route(&f.publisher, "orders", 1).await.shard();
    assert!(
        !f.publisher
            .inner
            .workers
            .iter()
            .any(|worker| std::ptr::eq(worker, &*own)),
        "a shard's stream is one of the pool's"
    );
    assert_eq!(f.opens.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn one_shard_keeps_one_writer_and_shards_do_not_share() {
    let f = fixture(4);
    let first = route(&f.publisher, "orders", 0).await.shard();
    let again = route(&f.publisher, "orders", 0).await.shard();
    assert!(Arc::ptr_eq(&first, &again), "a shard moved between writers");
    let other = route(&f.publisher, "orders", 1).await.shard();
    assert!(!Arc::ptr_eq(&first, &other), "two shards share a writer");
    let elsewhere = route(&f.publisher, "payments", 0).await.shard();
    assert!(
        !Arc::ptr_eq(&first, &elsewhere),
        "two streams share a writer"
    );
    assert_eq!(f.opens.load(Ordering::SeqCst), 3);
}

/// Concurrent first publishes to a shard wait for one open, not one each.
#[tokio::test]
async fn concurrent_first_publishes_open_once() {
    let f = fixture(4);
    let (a, b) = tokio::join!(
        route(&f.publisher, "orders", 2),
        route(&f.publisher, "orders", 2)
    );
    assert!(Arc::ptr_eq(&a.shard(), &b.shard()));
    assert_eq!(f.opens.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn past_the_cap_shards_fall_back_to_the_pool() {
    let f = fixture(2);
    let held = route(&f.publisher, "orders", 0).await.shard();
    route(&f.publisher, "orders", 1).await.shard();
    let Own::Pooled(pooled) = route(&f.publisher, "orders", 2).await else {
        panic!("a shard past the cap got a stream of its own");
    };
    // The pool slot is the one the stream hashes to, so it stays put.
    let expected = f
        .publisher
        .select_worker("t", "ns", "orders")
        .expect("pool") as *const PublishWorker;
    assert_eq!(pooled, expected);
    // Shards that already hold a stream keep it.
    assert!(Arc::ptr_eq(
        &held,
        &route(&f.publisher, "orders", 0).await.shard()
    ));
    assert_eq!(f.opens.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_shard_whose_writer_died_moves_to_a_new_stream() {
    let f = fixture(1);
    let first = route(&f.publisher, "orders", 0).await.shard();
    kill(&first).await;
    let replaced = route(&f.publisher, "orders", 0).await.shard();
    assert!(!Arc::ptr_eq(&first, &replaced), "stayed on a dead writer");
    assert!(!replaced.tx.is_closed());
    // The replacement took the dead writer's slot, not a new one.
    assert!(matches!(
        route(&f.publisher, "orders", 1).await,
        Own::Pooled(_)
    ));
    assert!(Arc::ptr_eq(
        &replaced,
        &route(&f.publisher, "orders", 0).await.shard()
    ));
    f.publisher
        .publish("t", "ns", "orders", b"p".to_vec(), AckMode::PerMessage)
        .await
        .expect("publish on the replacement");
    assert_eq!(f.opens.load(Ordering::SeqCst), 2);
}

/// An open that fails fails the publish instead of sending it through the
/// pool, and the shard's next publish tries again.
#[tokio::test]
async fn a_failed_open_fails_the_publish_and_is_retried() {
    let f = fixture(4);
    f.fail.store(true, Ordering::SeqCst);
    assert!(
        f.publisher
            .publish("t", "ns", "orders", b"p".to_vec(), AckMode::PerMessage)
            .await
            .is_err()
    );
    f.fail.store(false, Ordering::SeqCst);
    route(&f.publisher, "orders", 0).await.shard();
    assert_eq!(f.opens.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unkeyed_and_idempotent_publishes_go_on_shard_zeros_stream() {
    let f = fixture(4);
    let zero = route(&f.publisher, "orders", 0).await.shard();
    let before = zero.request_counter.load(Ordering::SeqCst);
    f.publisher
        .publish("t", "ns", "orders", b"p".to_vec(), AckMode::PerMessage)
        .await
        .expect("publish");
    f.publisher
        .publish_idempotent_batch("t", "ns", "orders", vec![b"p".to_vec()], 7, 0)
        .await
        .expect("idempotent publish");
    assert_eq!(zero.request_counter.load(Ordering::SeqCst), before + 2);
    assert_eq!(f.opens.load(Ordering::SeqCst), 1);
}

/// `finish` ends the shard streams too, and a finished client opens no more.
#[tokio::test]
async fn finish_ends_shard_streams_and_opens_no_more() {
    let f = fixture(4);
    let own = route(&f.publisher, "orders", 0).await.shard();
    f.publisher.finish().await.expect("finish");
    assert!(own.tx.is_closed());
    assert!(
        f.publisher
            .publish("t", "ns", "orders", b"p".to_vec(), AckMode::PerMessage)
            .await
            .is_err()
    );
    assert_eq!(f.opens.load(Ordering::SeqCst), 1);
}
