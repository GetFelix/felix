//! Keyed publishes from a publisher that learns stream widths: which shard a
//! key goes on, and that the answer never changes once learned.
//!
//! A healthy shard rides the pool, so the shard a key landed on shows in its
//! ticket, which names the shard's place, rather than in its writer.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

use felix_wire::routing::{ShardRouting, shard_for_routing};

use super::fake_worker;
use crate::publish::routing::Selected;
use crate::publish::shard_streams::{STALL_FLOOR, now_nanos};
use crate::publish::writer::PublishWorker;
use crate::publish::{
    LearnWidth, OpenWorker, PublishSharding, Publisher, PublisherInner, StreamWidths,
};

const SHARDS: u32 = 4;

struct Fixture {
    publisher: Publisher,
    /// Streams opened for shards of their own.
    opens: Arc<AtomicUsize>,
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
    let opens = Arc::new(AtomicUsize::new(0));
    let open: OpenWorker = {
        let opens = Arc::clone(&opens);
        Arc::new(move || {
            opens.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(fake_worker()) })
        })
    };
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
        opens,
        learned,
        fail,
        width,
    }
}

/// The shard a publish with `key` to `stream` is routed as, by its ticket.
/// The shard is healthy, so the publish rides the pool.
async fn shard_for(publisher: &Publisher, stream: &str, key: Option<&[u8]>) -> u64 {
    let routed = publisher
        .route("t", "ns", stream, key, None)
        .await
        .expect("route");
    assert!(
        matches!(routed.worker, Selected::Pooled(_)),
        "a healthy shard left the pool"
    );
    routed.ticket.as_ref().expect("a ticket").id()
}

/// Move the shard of `key` to a stream of its own: make its publish overdue
/// at its pooled stream's head, publish to it again, then answer the head.
async fn move_shard(publisher: &Publisher, stream: &str, key: &[u8]) -> Arc<PublishWorker> {
    let id = shard_for(publisher, stream, Some(key)).await;
    let pooled = publisher
        .select_worker("t", "ns", stream)
        .expect("pooled writer");
    pooled.head.set(Some(id), now_nanos());
    tokio::time::advance(STALL_FLOOR * 2).await;
    let own = own_writer(publisher, stream, key).await;
    pooled.head.set(None, 0);
    own
}

/// The writer a publish with `key` goes to, which must be its shard's own.
async fn own_writer(publisher: &Publisher, stream: &str, key: &[u8]) -> Arc<PublishWorker> {
    match publisher
        .route("t", "ns", stream, Some(key), None)
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

/// Keys on one shard share its place, keys on different shards do not, an
/// unkeyed publish is shard 0's, and no healthy shard costs a stream.
#[tokio::test]
async fn a_keyed_publish_goes_on_its_shard() {
    let f = fixture();
    let first = shard_for(&f.publisher, "orders", Some(&key_on(1, 0))).await;
    let same = shard_for(&f.publisher, "orders", Some(&key_on(1, 1))).await;
    assert_eq!(first, same, "one key's shard is two places");
    let other = shard_for(&f.publisher, "orders", Some(&key_on(2, 0))).await;
    assert_ne!(first, other, "two shards share a place");
    let zero = shard_for(&f.publisher, "orders", Some(&key_on(0, 0))).await;
    let unkeyed = shard_for(&f.publisher, "orders", None).await;
    assert_eq!(zero, unkeyed, "shard 0 is two places");
    assert_eq!(
        f.learned.load(Ordering::SeqCst),
        1,
        "width asked more than once"
    );
    assert_eq!(
        f.opens.load(Ordering::SeqCst),
        0,
        "a healthy shard opened a stream"
    );
}

/// A shard moved to a stream of its own takes every key on it there, and
/// only those: the other shards stay pooled.
#[tokio::test(start_paused = true)]
async fn a_moved_shard_takes_all_its_keys() {
    let f = fixture();
    let first = move_shard(&f.publisher, "orders", &key_on(1, 0)).await;
    for skip in 1..8 {
        let writer = own_writer(&f.publisher, "orders", &key_on(1, skip)).await;
        assert!(Arc::ptr_eq(&first, &writer), "one shard on two writers");
    }
    for shard in [0, 2, 3] {
        shard_for(&f.publisher, "orders", Some(&key_on(shard, 0))).await;
    }
    assert_eq!(f.opens.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_first_publishes_ask_the_width_once() {
    let f = fixture();
    let key = key_on(3, 0);
    let (a, b, c) = tokio::join!(
        shard_for(&f.publisher, "orders", Some(&key)),
        shard_for(&f.publisher, "orders", Some(&key)),
        shard_for(&f.publisher, "orders", Some(&key)),
    );
    assert!(a == b && b == c);
    assert_eq!(f.learned.load(Ordering::SeqCst), 1);
}

/// A width that could not be learned keeps every key of the stream on one
/// writer, and is not asked again: a later answer would move shards to new
/// writers while publishes on the old ones are still in flight.
#[tokio::test]
async fn an_unlearned_width_keeps_the_stream_on_one_writer_for_good() {
    let f = fixture();
    f.fail.store(true, Ordering::SeqCst);
    let first = shard_for(&f.publisher, "orders", Some(&key_on(1, 0))).await;
    f.fail.store(false, Ordering::SeqCst);
    for shard in 0..SHARDS {
        let place = shard_for(&f.publisher, "orders", Some(&key_on(shard, 0))).await;
        assert_eq!(first, place, "shard {shard} left the writer");
    }
    assert_eq!(f.learned.load(Ordering::SeqCst), 1);
    // Another stream asks for its own.
    shard_for(&f.publisher, "payments", Some(b"k")).await;
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

/// The shard comes from the width the client keeps, never from the shard a
/// caller worked out. An idempotent producer or a `Client` that believes
/// another width still shares the plain publishes' writer for a key, pooled
/// or moved.
#[tokio::test(start_paused = true)]
async fn a_callers_own_shard_never_picks_a_second_writer() {
    let f = fixture();
    let key = key_on(1, 0);
    let plain = shard_for(&f.publisher, "orders", Some(&key)).await;
    for believed in [0, 2, 3, 7] {
        let routed = f
            .publisher
            .route("t", "ns", "orders", Some(&key), Some(believed))
            .await
            .expect("route");
        assert!(matches!(routed.worker, Selected::Pooled(_)));
        assert_eq!(
            routed.ticket.as_ref().expect("a ticket").id(),
            plain,
            "a caller that believed shard {believed} got a second place for the key"
        );
    }
    let own = move_shard(&f.publisher, "orders", &key).await;
    for believed in [0, 2, 3, 7] {
        let Selected::Shard(routed) = f
            .publisher
            .route("t", "ns", "orders", Some(&key), Some(believed))
            .await
            .expect("route")
            .worker
        else {
            panic!("a caller that believed shard {believed} was routed to the pool");
        };
        assert!(Arc::ptr_eq(&own, &routed));
    }
}

/// A stream deleted and created again with another width: the broker's
/// `not_found` drops the kept width, so the new stream's shards are worked
/// out from its own width and keys sharing a new shard share its writer.
#[tokio::test]
async fn a_stream_reported_gone_is_asked_its_width_again() {
    let f = fixture();
    shard_for(&f.publisher, "orders", Some(&key_on(1, 0))).await;
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
    shard_for(&f.publisher, "orders", Some(&key_on(1, 0))).await;
    assert_eq!(
        f.learned.load(Ordering::SeqCst),
        1,
        "width dropped too early"
    );

    f.publisher
        .forget_width_if_gone("t", "ns", "orders", Some(b"k"), &gone);
    // Every key on new shard 1 shares one place, whichever old shard it
    // was on.
    let first = shard_for(&f.publisher, "orders", Some(&key_of(2, 1, 0))).await;
    for skip in 1..8 {
        let place = shard_for(&f.publisher, "orders", Some(&key_of(2, 1, skip))).await;
        assert_eq!(first, place, "new shard 1 is two places");
    }
    assert_eq!(f.learned.load(Ordering::SeqCst), 2);
}

/// A `stale_width` refusal drops the kept width like `not_found` does: the
/// broker saw the batch routed by a width the stream no longer has.
#[tokio::test]
async fn a_stale_width_refusal_drops_the_kept_width() {
    let f = fixture();
    shard_for(&f.publisher, "orders", Some(&key_on(1, 0))).await;
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
    let first = shard_for(&f.publisher, "orders", Some(&key_of(2, 1, 0))).await;
    for skip in 1..8 {
        let place = shard_for(&f.publisher, "orders", Some(&key_of(2, 1, skip))).await;
        assert_eq!(first, place, "new shard 1 is two places");
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
    let before = shard_for(&f.publisher, "orders", Some(&key_on(1, 0))).await;
    f.width.store(2, Ordering::SeqCst);
    // Due at once, so this publish starts the check in the background and
    // is itself still routed by the old width.
    let still = shard_for(&f.publisher, "orders", Some(&key_on(1, 0))).await;
    assert_eq!(before, still);
    let mut taken = false;
    for _ in 0..100 {
        tokio::task::yield_now().await;
        let first = shard_for(&f.publisher, "orders", Some(&key_of(2, 1, 0))).await;
        if all_on(&f.publisher, &first).await {
            taken = true;
            break;
        }
    }
    assert!(taken, "the new width was never taken");
    assert!(f.learned.load(Ordering::SeqCst) >= 2);
}

/// Every key on new shard 1 of a two-shard stream goes to `first`.
async fn all_on(publisher: &Publisher, first: &u64) -> bool {
    for skip in 0..8 {
        if shard_for(publisher, "orders", Some(&key_of(2, 1, skip))).await != *first {
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
        shard_for(&f.publisher, "orders", Some(&key_on(shard, 0))).await;
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

/// A publisher whose pooled streams take widths and whose shard streams,
/// to a broker that did not negotiate `FEATURE_PUBLISH_WIDTH`, do not.
fn unchecked_shard_streams() -> Publisher {
    let learn: LearnWidth =
        Arc::new(|_, _, _| Box::pin(async { Ok((SHARDS, ShardRouting::Modulo)) }));
    let open: OpenWorker = Arc::new(|| {
        Box::pin(async {
            let mut worker = fake_worker();
            worker.takes_width = false;
            Ok(worker)
        })
    });
    Publisher {
        inner: Arc::new(PublisherInner::with_stream_widths(
            Arc::new(vec![fake_worker()]),
            16,
            open,
            StreamWidths::new(learn, true),
        )),
    }
}

/// A shard stream to a broker that did not negotiate `FEATURE_PUBLISH_WIDTH`
/// gets no `publish_width`, even when the client's other streams take one:
/// it could not decode the frame and would close the stream.
#[tokio::test(start_paused = true)]
async fn a_stream_that_did_not_negotiate_widths_gets_no_declaration() {
    let publisher = unchecked_shard_streams();
    let key = key_on(1, 0);
    let pooled = publisher
        .route("t", "ns", "orders", Some(&key), None)
        .await
        .expect("route");
    assert!(pooled.declaration.is_some(), "the pool takes widths");
    drop(pooled);
    move_shard(&publisher, "orders", &key).await;
    let routed = publisher
        .route("t", "ns", "orders", Some(&key), None)
        .await
        .expect("route");
    assert!(matches!(routed.worker, Selected::Shard(_)));
    assert!(
        routed.declaration.is_none(),
        "declared to a stream that cannot read it"
    );
}

/// A caller's width routes a batch only on a stream whose broker checks it.
/// When the caller's shard has moved to a stream whose broker lacks the bit,
/// the batch goes by the client's own width instead, as every other publish
/// to that broker does, so the two cannot put one key on two writers with
/// nothing to refuse either.
#[tokio::test(start_paused = true)]
async fn a_callers_width_does_not_route_on_a_stream_that_cannot_check_it() {
    let publisher = unchecked_shard_streams();
    let key = (0..)
        .map(|i: u32| format!("key-{i}").into_bytes())
        .find(|key| {
            shard_for_routing(ShardRouting::Modulo, SHARDS, Some(key))
                != shard_for_routing(ShardRouting::Modulo, 2, Some(key))
        })
        .expect("a key that moves");
    let own = shard_for(&publisher, "orders", Some(&key)).await;
    // The shard the caller's width puts the key on, moved to an unchecked
    // stream.
    let believed = shard_for_routing(ShardRouting::Modulo, 2, Some(&key));
    move_shard(&publisher, "orders", &key_on(believed, 0)).await;
    let callers = Arc::new(crate::publish::Width {
        shards: 2,
        routing: ShardRouting::Modulo,
        declaration: Some(bytes::Bytes::from_static(b"declared")),
        learned: true,
    });
    let routed = publisher
        .route_by("t", "ns", "orders", Some(&key), None, Some(callers))
        .await
        .expect("route");
    assert!(
        matches!(routed.worker, Selected::Pooled(_)),
        "a caller's unchecked width picked a second writer"
    );
    assert_eq!(routed.ticket.as_ref().expect("a ticket").id(), own);
    assert_ne!(
        routed.declaration.as_deref(),
        Some(b"declared".as_slice()),
        "declared the caller's width"
    );
}
