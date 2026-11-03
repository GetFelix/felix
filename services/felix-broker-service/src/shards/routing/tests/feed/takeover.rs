//! A broker taking over many shards at once, as after a failover: the opens
//! run side by side, and each shard's route moves here once its own open is
//! done.
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicUsize;

use super::*;
use crate::shards::lifecycle::{OPEN_CONCURRENCY, ShardStore};

/// A store whose opens wait until the test lets each one finish.
#[derive(Default)]
struct GatedStore {
    in_flight: AtomicUsize,
    most_in_flight: AtomicUsize,
    gates: StdMutex<HashMap<u32, Arc<tokio::sync::Semaphore>>>,
}

impl GatedStore {
    fn gate(&self, shard: u32) -> Arc<tokio::sync::Semaphore> {
        Arc::clone(
            self.gates
                .lock()
                .expect("gates")
                .entry(shard)
                .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(0))),
        )
    }

    fn finish(&self, shard: u32) {
        self.gate(shard).add_permits(1);
    }

    fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }
}

#[async_trait::async_trait]
impl ShardStore for GatedStore {
    async fn open(
        &self,
        key: &ShardKey,
        _generation: u64,
        _incarnation: u64,
        _begins_here: bool,
    ) -> anyhow::Result<()> {
        let now = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        self.most_in_flight.fetch_max(now, Ordering::AcqRel);
        let gate = self.gate(key.shard);
        gate.acquire().await.expect("gate").forget();
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        Ok(())
    }

    async fn release(&self, _key: &ShardKey) -> anyhow::Result<()> {
        Ok(())
    }
}

/// `count` shards led by broker-b at generation 1, then moved to this
/// broker, broker-a, at generation 2, as a failover would.
async fn take_over(
    count: u32,
) -> (
    Arc<GatedStore>,
    Arc<IngressRouter>,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let before: Vec<_> = (0..count).map(|n| assignment(n, "broker-b", 1)).collect();
    let store = Arc::new(GatedStore::default());
    let (state, _router) = state_with(&before, Arc::clone(&store) as Arc<dyn ShardStore>);
    let ingress = Arc::clone(&state.ingress);
    let ownership = Arc::clone(&state.ownership);
    let assignments_changed = Arc::clone(&state.assignments_changed);
    let shutdown = CancellationToken::new();
    let feed = spawn_feed(state, None, Duration::from_secs(60), shutdown.clone());
    until("the first tick to route to broker-b", || {
        ingress
            .route(&key(0))
            .is_some_and(|route| route.leader.node_id == "broker-b")
    })
    .await;

    {
        let mut ownership = ownership.write().await;
        for n in 0..count {
            ownership.apply(&key(n), Some(assignment(n, "broker-a", 2)));
        }
    }
    assignments_changed.notify_one();
    (store, ingress, shutdown, feed)
}

/// **Taken-over shards open side by side, never more than the bound at
/// once.** One at a time, a failover's hundreds of opens each wait out the
/// fsyncs of the one before.
#[tokio::test]
async fn taken_over_shards_open_side_by_side_up_to_the_bound() {
    let count = 2 * OPEN_CONCURRENCY as u32;
    let (store, ingress, shutdown, feed) = take_over(count).await;

    until("the bound's worth of opens to be in flight", || {
        store.in_flight() == OPEN_CONCURRENCY
    })
    .await;
    for n in 0..count {
        store.finish(n);
    }
    until("every shard to serve here", || {
        (0..count).all(|n| ingress.dispatch(&key(n)) == Dispatch::Local { generation: 2 })
    })
    .await;
    assert_eq!(
        store.most_in_flight.load(Ordering::Acquire),
        OPEN_CONCURRENCY
    );

    shutdown.cancel();
    let _ = feed.await;
}

/// **A taken-over shard serves once its own open is done**, not once the
/// whole pass is, and no shard's route names this broker before its open
/// is done: replication reads the routes, and would ship from a log that is
/// not open yet.
#[tokio::test]
async fn each_route_moves_here_only_once_its_shard_is_open() {
    let (store, ingress, shutdown, feed) = take_over(4).await;
    until("every open to be in flight", || store.in_flight() == 4).await;

    store.finish(0);
    until("shard 0 to serve here", || {
        ingress.dispatch(&key(0)) == Dispatch::Local { generation: 2 }
    })
    .await;
    for n in 1..4 {
        let route = ingress.route(&key(n)).expect("route");
        assert_eq!(
            (route.leader.node_id.as_str(), route.generation),
            ("broker-b", 1),
            "shard {n}'s route moved here before its open finished",
        );
    }

    for n in 1..4 {
        store.finish(n);
    }
    until("every shard to serve here", || {
        (0..4).all(|n| ingress.dispatch(&key(n)) == Dispatch::Local { generation: 2 })
    })
    .await;

    shutdown.cancel();
    let _ = feed.await;
}
