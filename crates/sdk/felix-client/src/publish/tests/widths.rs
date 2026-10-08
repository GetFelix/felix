//! Keyed publishes from a publisher that learns stream widths: which shard's
//! stream a key goes on, and that the answer never changes once learned.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use felix_wire::routing::{ShardRouting, shard_for_routing};

use super::fake_worker;
use crate::publish::routing::Selected;
use crate::publish::writer::PublishWorker;
use crate::publish::{LearnWidth, OpenWorker, PublishSharding, Publisher, PublisherInner};

const SHARDS: u32 = 4;

struct Fixture {
    publisher: Publisher,
    learned: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
}

fn fixture() -> Fixture {
    let learned = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicBool::new(false));
    let learn: LearnWidth = {
        let learned = Arc::clone(&learned);
        let fail = Arc::clone(&fail);
        Arc::new(move |_, _, _| {
            let learned = Arc::clone(&learned);
            let fail = Arc::clone(&fail);
            Box::pin(async move {
                learned.fetch_add(1, Ordering::SeqCst);
                // Long enough for concurrent first publishes to overlap.
                tokio::task::yield_now().await;
                anyhow::ensure!(!fail.load(Ordering::SeqCst), "width refused");
                Ok((SHARDS, ShardRouting::Modulo))
            })
        })
    };
    let open: OpenWorker = Arc::new(|| Box::pin(async { Ok(fake_worker()) }));
    let pool = Arc::new(vec![fake_worker(), fake_worker()]);
    Fixture {
        publisher: Publisher {
            inner: Arc::new(PublisherInner::with_widths(pool, 16, open, learn)),
        },
        learned,
        fail,
    }
}

/// The writer a publish with `key` to `stream` goes to.
async fn writer_for(publisher: &Publisher, stream: &str, key: Option<&[u8]>) -> Arc<PublishWorker> {
    let shard = publisher.shard_of("t", "ns", stream, key, None).await;
    match publisher
        .route("t", "ns", stream, shard)
        .await
        .expect("route")
    {
        Selected::Shard(worker) => worker,
        Selected::Pooled(_) => panic!("routed to the pool"),
    }
}

fn key_on(shard: u32, skip: usize) -> Vec<u8> {
    (0..)
        .map(|i: u32| format!("key-{i}").into_bytes())
        .filter(|key| shard_for_routing(ShardRouting::Modulo, SHARDS, Some(key)) == shard)
        .nth(skip)
        .expect("a key on the shard")
}

/// Keys on one shard share its writer, keys on different shards do not, and
/// an unkeyed publish is shard 0's.
#[tokio::test]
async fn a_keyed_publish_goes_on_its_shards_stream() {
    let f = fixture();
    let first = writer_for(&f.publisher, "orders", Some(&key_on(1, 0))).await;
    let same = writer_for(&f.publisher, "orders", Some(&key_on(1, 1))).await;
    assert!(Arc::ptr_eq(&first, &same), "one shard on two writers");
    let other = writer_for(&f.publisher, "orders", Some(&key_on(2, 0))).await;
    assert!(!Arc::ptr_eq(&first, &other), "two shards share a writer");
    let zero = writer_for(&f.publisher, "orders", Some(&key_on(0, 0))).await;
    let unkeyed = writer_for(&f.publisher, "orders", None).await;
    assert!(Arc::ptr_eq(&zero, &unkeyed), "shard 0 on two writers");
    assert_eq!(
        f.learned.load(Ordering::SeqCst),
        1,
        "width asked more than once"
    );
}

#[tokio::test]
async fn concurrent_first_publishes_ask_the_width_once() {
    let f = fixture();
    let key = key_on(3, 0);
    let (a, b, c) = tokio::join!(
        writer_for(&f.publisher, "orders", Some(&key)),
        writer_for(&f.publisher, "orders", Some(&key)),
        writer_for(&f.publisher, "orders", Some(&key)),
    );
    assert!(Arc::ptr_eq(&a, &b) && Arc::ptr_eq(&b, &c));
    assert_eq!(f.learned.load(Ordering::SeqCst), 1);
}

/// A width that could not be learned keeps every key of the stream on one
/// writer, and is not asked again: a later answer would move shards to new
/// writers while publishes on the old ones are still in flight.
#[tokio::test]
async fn an_unlearned_width_keeps_the_stream_on_one_writer_for_good() {
    let f = fixture();
    f.fail.store(true, Ordering::SeqCst);
    let first = writer_for(&f.publisher, "orders", Some(&key_on(1, 0))).await;
    f.fail.store(false, Ordering::SeqCst);
    for shard in 0..SHARDS {
        let writer = writer_for(&f.publisher, "orders", Some(&key_on(shard, 0))).await;
        assert!(
            Arc::ptr_eq(&first, &writer),
            "shard {shard} left the writer"
        );
    }
    assert_eq!(f.learned.load(Ordering::SeqCst), 1);
    // Another stream asks for its own.
    writer_for(&f.publisher, "payments", Some(b"k")).await;
    assert_eq!(f.learned.load(Ordering::SeqCst), 2);
}

/// Round-robin spreads every publish anyway, so it never pays for a width.
#[tokio::test]
async fn round_robin_does_not_ask_the_width() {
    let learned = Arc::new(AtomicUsize::new(0));
    let learn: LearnWidth = {
        let learned = Arc::clone(&learned);
        Arc::new(move |_, _, _| {
            learned.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok((SHARDS, ShardRouting::Modulo)) })
        })
    };
    let open: OpenWorker = Arc::new(|| Box::pin(async { Ok(fake_worker()) }));
    let mut inner = PublisherInner::with_widths(Arc::new(vec![fake_worker()]), 16, open, learn);
    inner.sharding = PublishSharding::RoundRobin;
    let publisher = Publisher {
        inner: Arc::new(inner),
    };
    assert_eq!(
        publisher
            .shard_of("t", "ns", "orders", Some(b"k"), None)
            .await,
        None
    );
    assert_eq!(learned.load(Ordering::SeqCst), 0);
}
