use std::sync::atomic::AtomicUsize;

use super::*;

struct Counted {
    reads: AtomicUsize,
}

#[async_trait::async_trait]
impl ConfirmedRoutes for Counted {
    async fn confirmed(&self, _key: &ShardKey) -> Confirmed {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Confirmed::Assigned {
            generation: 1,
            leader: "broker-a".to_string(),
        }
    }
}

fn key() -> ShardKey {
    ShardKey {
        tenant_id: "t".to_string(),
        namespace: "n".to_string(),
        stream: "s".to_string(),
        shard: 0,
        kind: felix_router::ShardKind::Stream,
    }
}

/// Polls `decide` until it answers, for at most `within`.
async fn answered_within(
    reads: &Arc<ConfirmedReads>,
    routes: &Arc<dyn ConfirmedRoutes>,
    fresh: Duration,
    within: Duration,
) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if reads.decide(routes, &key(), 7, fresh, |_| ()).is_some() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

/// **A request needing a shorter freshness gets a new read promptly
/// (#1791).** The task holding a 10 s answer would otherwise sleep most of
/// those 10 s, and every request needing 500 ms (elections turned on) would
/// be refused until then.
#[tokio::test]
async fn a_shorter_freshness_wakes_the_refresh() {
    let counted = Arc::new(Counted {
        reads: AtomicUsize::new(0),
    });
    let routes: Arc<dyn ConfirmedRoutes> = Arc::clone(&counted) as _;
    let reads = Arc::new(ConfirmedReads::default());
    assert!(answered_within(&reads, &routes, FRESH, Duration::from_secs(2)).await);
    tokio::time::sleep(FRESH_UNDER_ELECTIONS + Duration::from_millis(100)).await;
    assert!(
        reads
            .decide(&routes, &key(), 7, FRESH_UNDER_ELECTIONS, |_| ())
            .is_none(),
        "the answer is older than the freshness asked for"
    );
    assert!(
        answered_within(
            &reads,
            &routes,
            FRESH_UNDER_ELECTIONS,
            Duration::from_secs(2)
        )
        .await,
        "refused until the 10 s refresh"
    );
    assert_eq!(counted.reads.load(Ordering::SeqCst), 2);
}

fn shard(index: u32) -> ShardKey {
    ShardKey {
        shard: index,
        ..key()
    }
}

fn counted() -> (Arc<Counted>, Arc<dyn ConfirmedRoutes>) {
    let counted = Arc::new(Counted {
        reads: AtomicUsize::new(0),
    });
    let routes: Arc<dyn ConfirmedRoutes> = Arc::clone(&counted) as _;
    (counted, routes)
}

/// **A leader that retries on its driver tick finds the answer its first
/// request started (#1791).** A copy with no incarnation uses an answer for
/// 500 ms, so the reader has to keep it fresh until the retry comes. Kept
/// for only 5 s, a retry 10 s later was refused and started another read,
/// every time, and a move onto the copy never finished.
#[tokio::test(start_paused = true)]
async fn a_retry_a_driver_tick_later_is_answered() {
    let (_, routes) = counted();
    let tick = Duration::from_secs(10);
    let reads = Arc::new(ConfirmedReads::new(tick));
    assert!(
        reads
            .decide(&routes, &key(), 0, FRESH_UNDER_ELECTIONS, |_| ())
            .is_none(),
        "nothing read yet"
    );
    tokio::time::sleep(tick).await;
    assert!(
        reads
            .decide(&routes, &key(), 0, FRESH_UNDER_ELECTIONS, |_| ())
            .is_some(),
        "the retry was refused again"
    );
}

/// **A retry that came after its window is waited for longer the next
/// time (#1791).** A leader's fence can queue behind others for several
/// ticks; with a fixed window every such retry is refused and starts
/// another read.
#[tokio::test(start_paused = true)]
async fn a_retry_that_came_late_is_waited_for_longer() {
    let (_, routes) = counted();
    let reads = Arc::new(ConfirmedReads::new(Duration::from_secs(2)));
    let refused = |reads: &Arc<ConfirmedReads>| {
        reads
            .decide(&routes, &key(), 0, FRESH_UNDER_ELECTIONS, |_| ())
            .is_none()
    };
    assert!(refused(&reads), "nothing read yet");
    // Past the 5 s window: refused again.
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert!(refused(&reads), "the answer outlived its window");
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert!(
        !refused(&reads),
        "a retry as late as the last one was refused again"
    );
    // The leader went quiet for longer than the 16 s window, so the answer
    // lapsed. Its next refused request is still waited for as long.
    tokio::time::sleep(Duration::from_secs(20)).await;
    assert!(refused(&reads), "the answer outlived its window");
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert!(
        !refused(&reads),
        "the longer wait was lost once a request was answered"
    );
}

/// **A leader that keeps retrying is answered every time, not every other
/// time.** A copy that took the fence is asked again when the round failed
/// on another copy, and a learner checks every batch. When only a refused
/// request kept the answer fresh, an answered retry let it lapse, so under
/// elections a leader retrying every 2 s was refused every other round, and
/// two such copies out of step never took one fence together.
#[tokio::test(start_paused = true)]
async fn an_answered_retry_keeps_the_answer_fresh() {
    let (_, routes) = counted();
    let reads = Arc::new(ConfirmedReads::new(Duration::from_secs(2)));
    let refused = || {
        reads
            .decide(&routes, &key(), 0, FRESH_UNDER_ELECTIONS, |_| ())
            .is_none()
    };
    assert!(refused(), "nothing read yet");
    for retry in 1..=10 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(!refused(), "retry {retry} was refused");
    }
}

/// **A routing change naming a copy keeps a refused retry's longer
/// window (#1791).** When the leader asks before this broker's feed names
/// the copy, the early read's 5 s window must not replace the one the
/// retry is due in.
#[tokio::test(start_paused = true)]
async fn a_prefetch_keeps_a_refused_retrys_window() {
    let (_, routes) = counted();
    let tick = Duration::from_secs(10);
    let reads = Arc::new(ConfirmedReads::new(tick));
    assert!(
        reads
            .decide(&routes, &key(), 0, FRESH_UNDER_ELECTIONS, |_| ())
            .is_none(),
        "nothing read yet"
    );
    reads.prefetch(&routes, &key(), 0, FRESH_UNDER_ELECTIONS);
    tokio::time::sleep(tick + tick / 2).await;
    assert!(
        reads
            .decide(&routes, &key(), 0, FRESH_UNDER_ELECTIONS, |_| ())
            .is_some(),
        "the retry was refused again"
    );
}

/// **A copy the routing view names before the leader asks answers the
/// leader's first request (#1791).** Without the early read that request is
/// refused, and the leader is back only on its next tick.
#[tokio::test(start_paused = true)]
async fn a_prefetched_copy_answers_the_first_request() {
    let (_, routes) = counted();
    let reads = Arc::new(ConfirmedReads::new(Duration::from_secs(10)));
    let prefetch = Prefetch::new(Arc::clone(&reads), Arc::clone(&routes), None);
    prefetch.named([(key(), 0)]);
    // The leader learns of the copy and connects a moment later.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        reads
            .decide(&routes, &key(), 0, FRESH_UNDER_ELECTIONS, |_| ())
            .is_some(),
        "the first request was refused"
    );
}

/// **A placement naming hundreds of copies at once starts a bounded number
/// of early reads**, and naming one again starts no second read.
#[tokio::test(start_paused = true)]
async fn early_reads_are_bounded_per_change() {
    let (counted, routes) = counted();
    let reads = Arc::new(ConfirmedReads::new(Duration::from_secs(2)));
    let prefetch = Prefetch::new(Arc::clone(&reads), Arc::clone(&routes), None);
    prefetch.named((0..500).map(|index| (shard(index), 7)));
    assert_eq!(reads.slots.load().len(), PREFETCH_PER_CHANGE);
    prefetch.named([(shard(0), 7)]);
    // Long enough for every read to finish, well short of a refresh.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        counted.reads.load(Ordering::SeqCst),
        PREFETCH_PER_CHANGE,
        "one read per shard"
    );
}

/// **An early read nobody asks about stops after [`WANTED_FOR`]**, not the
/// longer wait for a refused request's retry, so reads for a cancelled move
/// do not hold permits that leaders' copies wait on.
#[tokio::test(start_paused = true)]
async fn an_unasked_early_read_stops() {
    let (counted, routes) = counted();
    let reads = Arc::new(ConfirmedReads::new(Duration::from_secs(10)));
    let prefetch = Prefetch::new(Arc::clone(&reads), Arc::clone(&routes), None);
    prefetch.named([(key(), 0)]);
    tokio::time::sleep(WANTED_FOR + Duration::from_secs(1)).await;
    let stopped = counted.reads.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(counted.reads.load(Ordering::SeqCst), stopped);
    assert!(!reads.slots.load()[&key()].reading.load(Ordering::SeqCst));
}

/// **A copy named again after its early read stopped reads again**, as for
/// a move cancelled and then re-issued to the same broker.
#[tokio::test(start_paused = true)]
async fn a_copy_named_again_reads_again() {
    let (_, routes) = counted();
    let reads = Arc::new(ConfirmedReads::new(Duration::from_secs(10)));
    let prefetch = Prefetch::new(Arc::clone(&reads), Arc::clone(&routes), None);
    prefetch.named([(key(), 0)]);
    tokio::time::sleep(WANTED_FOR * 2).await;
    prefetch.named([(key(), 0)]);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        reads
            .decide(&routes, &key(), 0, FRESH_UNDER_ELECTIONS, |_| ())
            .is_some(),
        "the second naming started no read"
    );
}

/// Answers after `latency`, one shard or many per request, as a control
/// plane a round trip away does.
struct Slow {
    latency: Duration,
}

#[async_trait::async_trait]
impl ConfirmedRoutes for Slow {
    async fn confirmed(&self, _key: &ShardKey) -> Confirmed {
        tokio::time::sleep(self.latency).await;
        Confirmed::Assigned {
            generation: 1,
            leader: "broker-a".to_string(),
        }
    }

    async fn confirmed_many(&self, keys: &[ShardKey]) -> Option<Vec<Confirmed>> {
        tokio::time::sleep(self.latency).await;
        Some(
            keys.iter()
                .map(|_| Confirmed::Assigned {
                    generation: 1,
                    leader: "broker-a".to_string(),
                })
                .collect(),
        )
    }
}

/// **Many learner copies stay fresh under elections behind a slow control
/// plane.** 400 shards each need an answer under 500 ms old, and a read takes
/// 100 ms. Sixteen single-shard requests at a time make 160 reads a second,
/// far short of what 400 shards need, so most batches would be refused
/// `StaleRoute` for good. Reads batched per request keep every one fresh.
#[tokio::test(start_paused = true)]
async fn many_learner_copies_stay_fresh_behind_a_slow_control_plane() {
    let routes: Arc<dyn ConfirmedRoutes> = Arc::new(Slow {
        latency: Duration::from_millis(100),
    });
    let reads = Arc::new(ConfirmedReads::default());
    let shards: Vec<ShardKey> = (0..400).map(shard).collect();
    let ask = |reads: &Arc<ConfirmedReads>| {
        shards
            .iter()
            .filter(|key| {
                reads
                    .decide(&routes, key, 0, FRESH_UNDER_ELECTIONS, |_| ())
                    .is_none()
            })
            .count()
    };
    // Warm up: every shard's first request starts its read.
    for _ in 0..30 {
        ask(&reads);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut refused = 0;
    for _ in 0..100 {
        refused += ask(&reads);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(refused, 0, "requests refused for want of a fresh answer");
}
