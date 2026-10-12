//! Keyed publishes from a publisher that learns stream widths: which shard's
//! stream a key goes on, and that the answer never changes once learned.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

use felix_wire::routing::{ShardRouting, shard_for_routing};

use super::fake_worker;
use crate::publish::routing::Selected;
use crate::publish::writer::PublishWorker;
use crate::publish::{
    LearnWidth, OpenWorker, PublishSharding, Publisher, PublisherInner, StreamWidths,
};

const SHARDS: u32 = 4;

struct Fixture {
    publisher: Publisher,
    learned: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
    /// The width the broker reports.
    width: Arc<AtomicU32>,
}

fn fixture() -> Fixture {
    fixture_checking(None)
}

/// A fixture whose widths are asked again `every` this long, or at the
/// default interval.
fn fixture_checking(every: Option<std::time::Duration>) -> Fixture {
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
            inner: Arc::new(PublisherInner::with_stream_widths(
                pool,
                16,
                open,
                match every {
                    Some(every) => StreamWidths::new(learn, true).checking_every(every),
                    None => StreamWidths::new(learn, true),
                },
            )),
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
        .worker
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
            .await
            .0,
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
            .worker
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

/// A `stale_width` refusal drops the kept width like `not_found` does: the
/// broker saw the batch routed by a width the stream no longer has.
#[tokio::test]
async fn a_stale_width_refusal_drops_the_kept_width() {
    let f = fixture();
    writer_for(&f.publisher, "orders", Some(&key_on(1, 0))).await;
    f.width.store(2, Ordering::SeqCst);
    let stale: anyhow::Result<()> = Err(crate::error::refused(
        "publish rejected",
        "routed by a width of 4 shards".to_string(),
        Some(felix_wire::ErrorCode::StaleWidth),
        None,
        None,
    ));
    f.publisher
        .forget_width_if_gone("t", "ns", "orders", Some(b"k"), &stale);
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

/// The width a keyed publish was routed by travels with it, so the broker
/// can refuse it if the stream has another. The one-shard fallback for a
/// width that could not be learned keeps the stream on one writer and
/// declares nothing.
#[tokio::test]
async fn a_keyed_publish_carries_the_width_it_was_routed_by() {
    let f = fixture();
    let routed = f
        .publisher
        .route("t", "ns", "orders", Some(&key_on(2, 0)), None)
        .await
        .expect("route");
    let declaration = routed.declaration.clone().expect("a declaration");
    let frame = felix_wire::Frame::decode(declaration).expect("frame");
    assert_eq!(
        felix_wire::Message::decode(frame).expect("message"),
        felix_wire::Message::PublishWidth {
            tenant_id: "t".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shards: SHARDS,
            routing: None,
        }
    );
    let unkeyed = f
        .publisher
        .route("t", "ns", "orders", None, None)
        .await
        .expect("route");
    assert!(unkeyed.declaration.is_none());

    f.fail.store(true, Ordering::SeqCst);
    let fallback = f
        .publisher
        .route("t", "ns", "payments", Some(b"k"), None)
        .await
        .expect("route");
    assert!(fallback.declaration.is_none());
}

/// The case nothing refuses: a stream deleted and created again two shards
/// wide while this client only sends fire-and-forget publishes, or is idle.
/// The width is asked again once due, and the new answer replaces the old,
/// so keys sharing a new shard share its writer.
#[tokio::test]
async fn a_checked_width_is_asked_again_and_a_new_answer_taken() {
    let f = fixture_checking(Some(std::time::Duration::ZERO));
    let before = writer_for(&f.publisher, "orders", Some(&key_on(1, 0))).await;
    f.width.store(2, Ordering::SeqCst);
    // Due at once, so this publish starts the check in the background and
    // is itself still routed by the old width.
    let still = writer_for(&f.publisher, "orders", Some(&key_on(1, 0))).await;
    assert!(Arc::ptr_eq(&before, &still));
    let mut taken = false;
    for _ in 0..100 {
        tokio::task::yield_now().await;
        let first = writer_for(&f.publisher, "orders", Some(&key_of(2, 1, 0))).await;
        if all_on(&f.publisher, &first).await {
            taken = true;
            break;
        }
    }
    assert!(taken, "the new width was never taken");
    assert!(f.learned.load(Ordering::SeqCst) >= 2);
}

/// Every key on new shard 1 of a two-shard stream goes to `first`.
async fn all_on(publisher: &Publisher, first: &Arc<PublishWorker>) -> bool {
    for skip in 0..8 {
        let writer = writer_for(publisher, "orders", Some(&key_of(2, 1, skip))).await;
        if !Arc::ptr_eq(first, &writer) {
            return false;
        }
    }
    true
}

/// Without a newer answer the kept width stays, and a width is not asked
/// again before it is due.
#[tokio::test]
async fn a_width_is_not_asked_again_before_it_is_due() {
    let f = fixture();
    for shard in 0..SHARDS {
        writer_for(&f.publisher, "orders", Some(&key_on(shard, 0))).await;
        tokio::task::yield_now().await;
    }
    assert_eq!(f.learned.load(Ordering::SeqCst), 1);
}

/// A re-ask that never answers is given up, and the width is asked again on
/// the next interval rather than never again.
#[tokio::test]
async fn a_re_ask_that_hangs_does_not_stop_the_checks() {
    let hang = Arc::new(AtomicBool::new(false));
    let width = Arc::new(AtomicU32::new(SHARDS));
    let learn: LearnWidth = {
        let hang = Arc::clone(&hang);
        let width = Arc::clone(&width);
        Arc::new(move |_, _, _| {
            let hang = hang.load(Ordering::SeqCst);
            let width = width.load(Ordering::SeqCst);
            Box::pin(async move {
                if hang {
                    std::future::pending::<()>().await;
                }
                Ok((width, ShardRouting::Modulo))
            })
        })
    };
    let widths = StreamWidths::new(learn, true)
        .checking_every(std::time::Duration::ZERO)
        .giving_up_after(std::time::Duration::from_millis(20));
    let key = key_on(1, 0);
    assert_eq!(
        widths.shard_of("t", "ns", "orders", &key).await.1.shards,
        SHARDS
    );
    hang.store(true, Ordering::SeqCst);
    // Starts a check that hangs.
    widths.shard_of("t", "ns", "orders", &key).await;
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    hang.store(false, Ordering::SeqCst);
    width.store(2, Ordering::SeqCst);
    for _ in 0..100 {
        if widths.shard_of("t", "ns", "orders", &key).await.1.shards == 2 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("the width was never asked again after a re-ask hung");
}

/// A shard stream to a broker that did not negotiate `FEATURE_PUBLISH_WIDTH`
/// gets no `publish_width`, even when the client's other streams take one:
/// it could not decode the frame and would close the stream.
#[tokio::test]
async fn a_stream_that_did_not_negotiate_widths_gets_no_declaration() {
    let learn: LearnWidth =
        Arc::new(|_, _, _| Box::pin(async { Ok((SHARDS, ShardRouting::Modulo)) }));
    let open: OpenWorker = Arc::new(|| {
        Box::pin(async {
            let mut worker = fake_worker();
            worker.takes_width = false;
            Ok(worker)
        })
    });
    let pool = Arc::new(vec![fake_worker()]);
    let publisher = Publisher {
        inner: Arc::new(PublisherInner::with_stream_widths(
            pool,
            16,
            open,
            StreamWidths::new(learn, true),
        )),
    };
    let routed = publisher
        .route("t", "ns", "orders", Some(&key_on(1, 0)), None)
        .await
        .expect("route");
    assert!(matches!(routed.worker, Selected::Shard(_)));
    assert!(
        routed.declaration.is_none(),
        "declared to a stream that cannot read it"
    );
}
