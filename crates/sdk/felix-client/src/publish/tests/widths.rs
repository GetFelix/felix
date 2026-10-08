//! Keyed publishes from a publisher that learns stream widths: which shard's
//! stream a key goes on, and that the answer never changes once learned.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

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
    /// The width the broker reports.
    width: Arc<AtomicU32>,
}

fn fixture() -> Fixture {
    let learned = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicBool::new(false));
    let width = Arc::new(AtomicU32::new(SHARDS));
    let learn: LearnWidth = {
        let learned = Arc::clone(&learned);
        let fail = Arc::clone(&fail);
        let width = Arc::clone(&width);
        Arc::new(move |_, _, _| {
            let learned = Arc::clone(&learned);
            let fail = Arc::clone(&fail);
            let width = width.load(Ordering::SeqCst);
            Box::pin(async move {
                learned.fetch_add(1, Ordering::SeqCst);
                // Long enough for concurrent first publishes to overlap.
                tokio::task::yield_now().await;
                anyhow::ensure!(!fail.load(Ordering::SeqCst), "width refused");
                Ok((width, ShardRouting::Modulo))
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
        width,
    }
}

/// The writer a publish with `key` to `stream` goes to.
async fn writer_for(publisher: &Publisher, stream: &str, key: Option<&[u8]>) -> Arc<PublishWorker> {
    match publisher
        .route("t", "ns", stream, key, None)
        .await
        .expect("route")
    {
        Selected::Shard(worker) => worker,
        Selected::Pooled(_) => panic!("routed to the pool"),
    }
}

fn key_on(shard: u32, skip: usize) -> Vec<u8> {
    key_of(SHARDS, shard, skip)
}

/// The `skip`th key on `shard` of a stream `shards` wide.
fn key_of(shards: u32, shard: u32, skip: usize) -> Vec<u8> {
    (0..)
        .map(|i: u32| format!("key-{i}").into_bytes())
        .filter(|key| shard_for_routing(ShardRouting::Modulo, shards, Some(key)) == shard)
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

/// The writer comes from the width the client keeps, never from the shard a
/// caller worked out. An idempotent producer or a `ClusterClient` that
/// believes another width still shares the plain publishes' writer for a key.
#[tokio::test]
async fn a_callers_own_shard_never_picks_a_second_writer() {
    let f = fixture();
    let key = key_on(1, 0);
    let plain = writer_for(&f.publisher, "orders", Some(&key)).await;
    for believed in [0, 2, 3, 7] {
        let routed = match f
            .publisher
            .route("t", "ns", "orders", Some(&key), Some(believed))
            .await
            .expect("route")
        {
            Selected::Shard(worker) => worker,
            Selected::Pooled(_) => panic!("routed to the pool"),
        };
        assert!(
            Arc::ptr_eq(&plain, &routed),
            "a caller that believed shard {believed} got a second writer for the key"
        );
    }
}

/// A stream deleted and created again with another width: the broker's
/// `not_found` drops the kept width, so the new stream's shards are worked
/// out from its own width and keys sharing a new shard share its writer.
#[tokio::test]
async fn a_stream_reported_gone_is_asked_its_width_again() {
    let f = fixture();
    writer_for(&f.publisher, "orders", Some(&key_on(1, 0))).await;
    // Created again two shards wide. A refusal of anything else keeps the
    // width.
    f.width.store(2, Ordering::SeqCst);
    let other: anyhow::Result<()> = Err(crate::error::refused(
        "publish rejected",
        "busy".to_string(),
        Some(felix_wire::ErrorCode::Overloaded),
        None,
        None,
    ));
    f.publisher
        .forget_width_if_gone("t", "ns", "orders", Some(b"k"), &other);
    let gone: anyhow::Result<()> = Err(crate::error::refused(
        "publish rejected",
        "no such stream".to_string(),
        Some(felix_wire::ErrorCode::NotFound),
        None,
        None,
    ));
    // An unkeyed publish never needed the width.
    f.publisher
        .forget_width_if_gone("t", "ns", "orders", None, &gone);
    assert_eq!(f.learned.load(Ordering::SeqCst), 1);
    writer_for(&f.publisher, "orders", Some(&key_on(1, 0))).await;
    assert_eq!(
        f.learned.load(Ordering::SeqCst),
        1,
        "width dropped too early"
    );

    f.publisher
        .forget_width_if_gone("t", "ns", "orders", Some(b"k"), &gone);
    // Every key on new shard 1 shares one writer, whichever old shard it
    // was on.
    let first = writer_for(&f.publisher, "orders", Some(&key_of(2, 1, 0))).await;
    for skip in 1..8 {
        let writer = writer_for(&f.publisher, "orders", Some(&key_of(2, 1, skip))).await;
        assert!(
            Arc::ptr_eq(&first, &writer),
            "new shard 1 is on two writers"
        );
    }
    assert_eq!(f.learned.load(Ordering::SeqCst), 2);
}
