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
