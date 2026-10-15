//! Which shards a pass ships, to whom, and how long it takes.

use super::*;

/// **Unreachable followers cost one handshake timeout, not one each.** A pass
/// that visited them one after another paid for every dead replica in turn,
/// with the quorum mark -- and so every `Quorum` publish waiting on this shard
/// -- behind the sum.
///
/// The clock is the assertion: visited sequentially, two unreachable followers
/// take twice as long as one, and the pass still finishes, just far too late to
/// be worth anything to a publish.
#[tokio::test(start_paused = true)]
async fn unreachable_followers_are_waited_on_at_the_same_time() {
    let handshake = std::time::Duration::from_secs(2);
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let requester = UnreachableFollowers { handshake };
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    let started = tokio::time::Instant::now();
    replicate_once(
        &requester,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    let took = started.elapsed();
    assert!(
        took < handshake * 2,
        "the followers were waited on one after another: {took:?}",
    );
}

/// The shard this broker leads is shipped to every follower in its set.
#[tokio::test]
async fn a_led_shard_is_shipped_to_each_follower() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    let mut shipped_to: Vec<String> = follower
        .batches()
        .into_iter()
        .map(|(node, _, _)| node)
        .collect();
    shipped_to.sort();
    assert_eq!(shipped_to, vec!["broker-b", "broker-c"]);
}

/// **A shard this broker only follows is not shipped from here.** Two brokers
/// both shipping the same shard would each be writing the other's log.
#[tokio::test]
async fn a_shard_led_elsewhere_is_not_shipped() {
    let (broker, _dir) = leader_with(3).await;
    let router = router("broker-b", &[LOCAL], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    assert!(
        follower.batches().is_empty(),
        "a follower shipped a shard it does not lead",
    );
}

/// A shard with no replicas has nowhere to ship, which is the unreplicated
/// default and must cost nothing.
#[tokio::test]
async fn a_shard_with_no_replicas_ships_nothing() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &[], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    assert!(follower.batches().is_empty());
}

/// A pass ships until the follower is level, so catching up is not rationed to
/// one batch per tick.
#[tokio::test]
async fn one_pass_ships_until_the_follower_is_level() {
    let (broker, _dir) = leader_with(5).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    let delivered: usize = follower.batches().iter().map(|(_, _, len)| len).sum();
    assert_eq!(delivered, 5, "the follower was left behind after a pass");
}

/// A second pass with nothing new ships nothing: the cursor is remembered.
#[tokio::test]
async fn a_second_pass_with_nothing_new_ships_nothing() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
    let after_first = follower.batches().len();
    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    assert_eq!(
        follower.batches().len(),
        after_first,
        "the cursor was forgotten between passes",
    );
}

/// A broker with no durable storage leads only ephemeral streams, which have no
/// log to ship.
#[tokio::test]
async fn a_broker_without_durable_storage_ships_nothing() {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    assert_eq!(
        replicate_once(
            &follower,
            &broker,
            &router,
            &marks,
            None,
            &mut cursors,
            &mut HashMap::new(),
            &mut HashMap::new(),
            &mut HashMap::new(),
        )
        .await
        .worst_lag,
        None,
    );
    assert!(follower.batches().is_empty());
}

/// The lag reported is the distance the slowest follower is behind the tail.
#[tokio::test]
async fn the_reported_lag_is_the_distance_from_the_tail() {
    let (broker, _dir) = leader_with(4).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    // Caught up after a full pass.
    assert_eq!(
        replicate_once(
            &follower,
            &broker,
            &router,
            &marks,
            None,
            &mut cursors,
            &mut HashMap::new(),
            &mut HashMap::new(),
            &mut HashMap::new(),
        )
        .await
        .worst_lag,
        Some(0),
    );
}

/// A durable append starts shipping without waiting for the tick.
///
/// The tick is seconds and the shipping is milliseconds, so under `Quorum` the
/// tick was most of a publish's latency — a record landed, and then the broker
/// waited out an interval before telling anyone about it (#411).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_append_ships_without_waiting_for_the_tick() {
    let (broker, _dir) = leader_with(0).await;
    // Registered on the broker, not just opened on disk: the point is that a
    // real publish wakes replication, and `publish_batch` needs the stream.
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            felix_broker::StreamMetadata {
                durable: true,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = Arc::new(AcceptingFollower::default());
    let marks = Arc::new(QuorumMarks::new());

    // A tick long enough that reaching it would mean the signal did nothing.
    let shutdown = CancellationToken::new();
    let driver = spawn(
        Arc::clone(&follower),
        Arc::clone(&broker),
        router,
        Arc::new(Unfenced),
        Arc::new(crate::promotion::NoGate),
        Published {
            marks: Arc::clone(&marks),
            halted: Arc::new(crate::halted::HaltedReplicas::new()),
            status: Arc::default(),
        },
        None,
        Duration::from_secs(300),
        Arc::default(),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        shutdown.clone(),
    );

    // The first pass runs on the interval's immediate first tick; let it
    // settle so what follows is attributable to the append.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = follower.batches().len();

    broker
        .publish_batch(TENANT, NAMESPACE, STREAM, 0, &[Bytes::from_static(b"now")])
        .await
        .expect("publish");

    let shipped = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if follower.batches().len() > before {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    shutdown.cancel();
    driver.stop().await;

    assert!(
        shipped.is_ok(),
        "nothing shipped within 5s of the append, against a 300s tick: the \
         append signal is not reaching the driver, so a Quorum publish waits \
         out the interval",
    );
}

/// The cache shard of the tests' stream name, led here at generation 4.
struct LedCache;

impl crate::quorum::ShardServing for LedCache {
    fn replicated(&self, _key: &crate::ShardKey) -> bool {
        true
    }

    fn generation(&self, _key: &crate::ShardKey) -> Option<u64> {
        Some(4)
    }

    fn lease_valid(&self) -> bool {
        true
    }

    fn record_ack_refusal(&self) {}
}

/// **A `Quorum` cache put acks without waiting for the tick.** The put went
/// to the log but never woke the driver, so every put waited out the tick
/// before it shipped (#928).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quorum_cache_put_ships_without_waiting_for_the_tick() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let broker = Arc::new(Broker::new(Box::new(
        felix_storage::LogCache::open(dir.path().join("caches"), config).expect("cache"),
    )));
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    broker
        .register_cache(
            TENANT,
            NAMESPACE,
            STREAM,
            felix_broker::CacheMetadata {
                consistency: felix_broker::ConsistencyLevel::Quorum,
            },
        )
        .await
        .expect("cache");
    let cache = ShardKey {
        kind: felix_router::ShardKind::Cache,
        ..key()
    };
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes = nodes();
    router.publish(
        RoutingTable::build(
            [(
                cache.clone(),
                LOCAL.to_string(),
                vec!["broker-b".to_string(), "broker-c".to_string()],
                4,
            )],
            &nodes,
        ),
        &nodes,
    );
    let marks = Arc::new(QuorumMarks::new());

    let shutdown = CancellationToken::new();
    let driver = spawn(
        Arc::new(AcceptingFollower::default()),
        Arc::clone(&broker),
        router,
        Arc::new(Unfenced),
        Arc::new(crate::promotion::NoGate),
        Published {
            marks: Arc::clone(&marks),
            halted: Arc::new(crate::halted::HaltedReplicas::new()),
            status: Arc::default(),
        },
        None,
        Duration::from_secs(300),
        Arc::default(),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        shutdown.clone(),
    );
    // Let the interval's immediate first tick pass.
    tokio::time::sleep(Duration::from_millis(200)).await;

    broker
        .cache()
        .put(
            TENANT,
            NAMESPACE,
            STREAM,
            0,
            "k",
            Bytes::from_static(b"v"),
            None,
        )
        .await
        .expect("put");
    let acked = crate::quorum::await_cache_quorum(
        &broker,
        &watch_key(&cache),
        Some(&marks),
        Some(&LedCache),
        Duration::from_secs(5),
        crate::quorum::Access::Write,
    )
    .await;

    shutdown.cancel();
    driver.stop().await;

    acked.expect(
        "the put was not on a majority within 5s, against a 300s tick: the \
         cache write is not waking the driver",
    );
}

/// A route change runs a pass without waiting for the tick.
///
/// A fenced shard's drained report and a new leader's first shipment both wait
/// on the next pass, and the control plane's next step of a move waits on
/// them. The routing feed says when it has acted on a change.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_route_change_ships_without_waiting_for_the_tick() {
    let (broker, _dir) = leader_with(3).await;
    // Led here with nobody to ship to, until the routes change.
    let router = router(LOCAL, &[], 4);
    let follower = Arc::new(AcceptingFollower::default());
    let routes_changed = Arc::new(tokio::sync::Notify::new());

    let shutdown = CancellationToken::new();
    let driver = spawn(
        Arc::clone(&follower),
        Arc::clone(&broker),
        Arc::clone(&router),
        Arc::new(Unfenced),
        Arc::new(crate::promotion::NoGate),
        Published {
            marks: Arc::new(QuorumMarks::new()),
            halted: Arc::new(crate::halted::HaltedReplicas::new()),
            status: Arc::default(),
        },
        None,
        Duration::from_secs(300),
        Arc::clone(&routes_changed),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        shutdown.clone(),
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(follower.batches().is_empty(), "nothing to ship to yet");

    publish(&router, LOCAL, &["broker-b"], 5);
    routes_changed.notify_one();

    let shipped = tokio::time::timeout(Duration::from_secs(5), async {
        while follower.batches().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    shutdown.cancel();
    driver.stop().await;
    assert!(
        shipped.is_ok(),
        "nothing shipped within 5s of the routes changing, against a 300s tick",
    );
}

/// A slow follower costs one delay for the whole pass, not one per shard.
///
/// A follower is slow for *every* shard it holds, so shipping shards one after
/// another multiplied its latency by the shard count — and every one of those
/// shards' quorum marks, and so every `Quorum` publish waiting on them, sat
/// behind the sum. One tenant's unlucky replica became every tenant's latency
/// (#411).
struct SlowFollower {
    delay: Duration,
    seen: Mutex<Vec<u32>>,
}

impl PeerRequester for SlowFollower {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let InternalMessage::ReplicateRecords(batch) = message else {
            panic!("the driver sent something other than a replication batch");
        };
        tokio::time::sleep(self.delay).await;
        self.seen.lock().expect("lock").push(batch.shard.shard);
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }
}

#[tokio::test(start_paused = true)]
async fn a_slow_follower_does_not_cost_one_delay_per_shard() {
    const SHARDS: u32 = 4;
    let (broker, _dir) = leader_with_shards(SHARDS, 2).await;
    let router = router_over_shards(LOCAL, &["broker-b"], 4, SHARDS);
    let delay = Duration::from_secs(30);
    let requester = SlowFollower {
        delay,
        seen: Mutex::new(Vec::new()),
    };
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    let started = tokio::time::Instant::now();
    replicate_once(
        &requester,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
    let took = started.elapsed();

    let seen = requester.seen.lock().expect("lock").clone();
    let mut distinct: Vec<u32> = seen.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        SHARDS as usize,
        "not every shard shipped: {seen:?}",
    );

    // Each shard sends two batches here, so sequentially this is 8 delays.
    // Concurrently it is 2 — the batches within a shard are still ordered.
    assert!(
        took < delay * 4,
        "the pass took {took:?} with a {delay:?} follower across {SHARDS} \
         shards; the shards were shipped one after another",
    );
}
