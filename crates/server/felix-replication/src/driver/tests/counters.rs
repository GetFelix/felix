//! A `Quorum` cache's counter log holds no mark on a follower that is gone.
//!
//! The counters ship beside the cache log, to each follower in its own
//! exchange, so one follower that does not answer is handed off like any
//! other straggler rather than waited for. The clock is paused, so "well under
//! a second" against a peer that takes seconds is exact.

use std::time::Duration;

use super::*;

/// How long a mark may take once there is a majority for it.
const PROMPT: Duration = Duration::from_millis(500);

const GENERATION: u64 = 4;

/// Stores every cache and counter batch at once, except to `gone`: what it is
/// sent there waits on [`Gone`] and then fails.
struct OneGone {
    gone: &'static str,
    how: Gone,
    /// Under [`Gone::Backoff`], when the next request dials again.
    dial_at: std::sync::Mutex<Option<tokio::time::Instant>>,
}

/// How a follower that is gone fails.
#[derive(Clone, Copy)]
enum Gone {
    /// The request sits on a half-open connection for a minute.
    Silent,
    /// As the pool fails a peer that refuses connections: a dial that gives
    /// up after `dial`, then a backoff of `backoff` in which requests fail at
    /// once, then a dial again.
    Backoff { dial: Duration, backoff: Duration },
}

impl OneGone {
    fn new(gone: &'static str, how: Gone) -> Self {
        Self {
            gone,
            how,
            dial_at: std::sync::Mutex::new(None),
        }
    }
}

impl PeerRequester for OneGone {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let batch = match message {
            InternalMessage::ReplicateRecords(batch)
            | InternalMessage::ReplicateCacheRecords(batch)
            | InternalMessage::ReplicateCounterRecords(batch) => batch,
            other => panic!("unexpected {:?}", other.kind()),
        };
        if node_id == self.gone {
            let unavailable = || PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "gone".to_string(),
            };
            match self.how {
                Gone::Silent => {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
                Gone::Backoff { dial, backoff } => {
                    let now = tokio::time::Instant::now();
                    let in_backoff = self
                        .dial_at
                        .lock()
                        .expect("lock")
                        .is_some_and(|at| now < at);
                    if !in_backoff {
                        tokio::time::sleep(dial).await;
                        *self.dial_at.lock().expect("lock") =
                            Some(tokio::time::Instant::now() + backoff);
                    }
                }
            }
            return Err(unavailable());
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }
}

fn cache_key() -> ShardKey {
    ShardKey {
        kind: felix_router::ShardKind::Cache,
        ..key()
    }
}

/// A broker leading a `Quorum` cache with a counter log, followed by b and c.
async fn quorum_cache() -> (Arc<Broker>, Arc<ShardRouter>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let counters = Arc::new(
        felix_storage::CounterStore::open(dir.path().join("counters"), config.clone())
            .expect("counters"),
    );
    let broker = Arc::new(
        Broker::new(Box::new(
            felix_storage::LogCache::open(dir.path().join("caches"), config.clone())
                .expect("cache"),
        ))
        .with_durable_storage(
            DurableStorage::open(dir.path().join("streams"), config).expect("storage"),
        )
        .with_counters(counters),
    );
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
                ..Default::default()
            },
        )
        .await
        .expect("cache");
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes = nodes();
    router.publish(
        RoutingTable::build(
            [(
                cache_key(),
                LOCAL.to_string(),
                vec!["broker-b".to_string(), "broker-c".to_string()],
                GENERATION,
            )],
            &nodes,
        ),
        &nodes,
    );
    (broker, router, dir)
}

/// Marks as a fleet that finalized acknowledging `Quorum` caches by their
/// followers, with both logs recording that this leader's generation began
/// at their current tails.
async fn by_followers(broker: &Broker) -> QuorumMarks {
    use felix_common::fleet::{FENCED_CACHES, FleetGate, GENERATION_START, MAJORITY_ACK};
    for kind in [
        felix_broker::LogKind::Cache,
        felix_broker::LogKind::Counters,
    ] {
        let log = broker
            .shard_log(kind, TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("log");
        let tail = log.tail_offset().await.expect("tail");
        log.record_generation(GENERATION, tail).expect("record");
    }
    let names = [
        GENERATION_START.name(),
        MAJORITY_ACK.name(),
        FENCED_CACHES.name(),
    ];
    let fleet = FleetGate::new(names);
    fleet.observe(names);
    QuorumMarks::with_fleet(Arc::new(fleet))
}

fn driver(
    requester: impl PeerRequester + Send + 'static,
    broker: &Arc<Broker>,
    router: &Arc<ShardRouter>,
    marks: QuorumMarks,
) -> (Replication, Arc<QuorumMarks>) {
    let marks = Arc::new(marks);
    let driver = spawn(
        Arc::new(requester),
        Arc::clone(broker),
        Arc::clone(router),
        Arc::new(Unfenced),
        Arc::new(crate::promotion::NoGate),
        Published {
            marks: Arc::clone(&marks),
            halted: Arc::new(crate::halted::HaltedReplicas::new()),
            status: Arc::default(),
        },
        None,
        // Reaching the tick would mean nothing else woke the driver.
        Duration::from_secs(300),
        Arc::default(),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        CancellationToken::new(),
    );
    (driver, marks)
}

/// Put a key and add to a counter, wake the driver as the write paths do, and
/// return the cache and counter logs' tails.
async fn write(broker: &Arc<Broker>, round: usize) -> (u64, u64) {
    // The writes wait on the logs' own threads, and a paused runtime with
    // nothing to poll jumps its clock to the next timer meanwhile, which would
    // be the gone follower's. Blocking work holds the clock, so wait from
    // there.
    let runtime = tokio::runtime::Handle::current();
    let broker_for_write = Arc::clone(broker);
    let tails = tokio::task::spawn_blocking(move || {
        runtime.block_on(async {
            let broker = broker_for_write;
            broker
                .cache()
                .put(
                    TENANT,
                    NAMESPACE,
                    STREAM,
                    0,
                    &format!("k{round}"),
                    Bytes::from_static(b"v"),
                    None,
                )
                .await
                .expect("put");
            broker
                .counters()
                .expect("counters")
                .add(TENANT, NAMESPACE, STREAM, 0, "hits", 1)
                .await
                .expect("add");
            let mut tails = Vec::new();
            for kind in [
                felix_broker::LogKind::Cache,
                felix_broker::LogKind::Counters,
            ] {
                let log = broker
                    .shard_log(kind, TENANT, NAMESPACE, STREAM, 0)
                    .await
                    .expect("log");
                tails.push(log.tail_offset().await.expect("tail"));
            }
            (tails[0], tails[1])
        })
    })
    .await
    .expect("write task");
    broker.appended().notify_one();
    tails
}

/// Whether both marks reach the tails within [`PROMPT`].
async fn prompt(marks: &QuorumMarks, (cache, counters): (u64, u64)) -> bool {
    let watched = watch_key(&cache_key());
    tokio::time::timeout(PROMPT, async {
        while marks.offset(&watched, GENERATION) < Some(cache)
            || marks.counters().offset(&watched, GENERATION) < Some(counters)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok()
}

/// **A follower that stopped answering holds no counter or cache mark.**
/// c sits on every request, as a broker that died behind a half-open
/// connection does. b is the majority for both logs, from the first write on.
#[tokio::test(start_paused = true)]
async fn a_silent_follower_holds_no_counter_mark() {
    let (broker, router, _dir) = quorum_cache().await;
    let (driver, marks) = driver(
        OneGone::new("broker-c", Gone::Silent),
        &broker,
        &router,
        QuorumMarks::new(),
    );

    for round in 0..3 {
        let tails = write(&broker, round).await;
        assert!(
            prompt(&marks, tails).await,
            "write {round}: the marks waited on the follower that does not answer",
        );
    }
    driver.stop().await;
}

/// **Nor does one that fails after a dial each time its backoff runs out.**
/// Once the first exchange with c ends, c is shipped to again on every pass,
/// and each time its backoff expires that costs a dial. Writes keep landing
/// across several of those.
#[tokio::test(start_paused = true)]
async fn a_follower_dialled_after_each_backoff_holds_no_counter_mark() {
    let (broker, router, _dir) = quorum_cache().await;
    let gone = Gone::Backoff {
        dial: Duration::from_secs(2),
        backoff: Duration::from_millis(700),
    };
    let (driver, marks) = driver(
        OneGone::new("broker-c", gone),
        &broker,
        &router,
        QuorumMarks::new(),
    );

    for round in 0..12 {
        let tails = write(&broker, round).await;
        assert!(
            prompt(&marks, tails).await,
            "write {round}: the marks waited on a dial to the follower that is gone",
        );
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    driver.stop().await;
}

/// **The same when the followers decide the marks.** A fleet that fences a
/// promoted cache shard acknowledges on what the followers answered at this
/// generation, with no report in the way; c not answering must hold neither
/// mark there either.
#[tokio::test(start_paused = true)]
async fn a_silent_follower_holds_no_counter_mark_the_followers_decide() {
    let (broker, router, _dir) = quorum_cache().await;
    let marks = by_followers(&broker).await;
    let (driver, marks) = driver(
        OneGone::new("broker-c", Gone::Silent),
        &broker,
        &router,
        marks,
    );

    for round in 0..3 {
        let tails = write(&broker, round).await;
        assert!(
            prompt(&marks, tails).await,
            "write {round}: the marks waited on the follower that does not answer",
        );
    }
    driver.stop().await;
}

/// **No majority, no counter mark.** Not waiting on a follower is not
/// counting it: with b and c both failing, neither the counter add nor the put
/// is acknowledged.
#[tokio::test(start_paused = true)]
async fn with_no_follower_answering_no_counter_add_is_acknowledged() {
    let (broker, router, _dir) = quorum_cache().await;
    let (driver, marks) = driver(
        UnreachableFollowers {
            handshake: Duration::from_millis(100),
        },
        &broker,
        &router,
        QuorumMarks::new(),
    );

    let (_, counters) = write(&broker, 0).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let watched = watch_key(&cache_key());
    assert!(
        marks.counters().offset(&watched, GENERATION).unwrap_or(0) < counters,
        "a counter add was acknowledged with no follower holding it",
    );
    driver.stop().await;
}

/// **The report carries where each follower is in the counter log.** A cache
/// used only for counters never moves its own log, so this is all placement
/// has to tell a restore copying counters from one that is stuck (#1164).
#[tokio::test]
async fn a_report_says_where_each_follower_is_in_the_counter_log() {
    let (broker, router, _dir) = quorum_cache().await;
    for _ in 0..3 {
        broker
            .counters()
            .expect("counters")
            .add(TENANT, NAMESPACE, STREAM, 0, "hits", 1)
            .await
            .expect("add");
    }
    let counters_tail = broker
        .shard_log(
            felix_broker::LogKind::Counters,
            TENANT,
            NAMESPACE,
            STREAM,
            0,
        )
        .await
        .expect("log")
        .tail_offset()
        .await
        .expect("tail");
    assert!(counters_tail > 0);

    let pass = replicate_once(
        &OneGone::new("nobody", Gone::Silent),
        &broker,
        &router,
        &QuorumMarks::new(),
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    let report = pass
        .reports
        .iter()
        .find(|report| report.key == cache_key())
        .expect("the cache was reported");
    assert_eq!(report.tail, 0, "the cache log holds nothing");
    let mut counters = report.counter_offsets.clone();
    counters.sort();
    assert_eq!(
        counters,
        [
            ("broker-b".to_string(), counters_tail),
            ("broker-c".to_string(), counters_tail),
        ]
    );
}

/// Takes one counter record per batch, slowly; everything else at once.
struct SlowCounters {
    per_batch: Duration,
}

impl PeerRequester for SlowCounters {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let durable_offset = match message {
            InternalMessage::ReplicateCounterRecords(batch) => {
                tokio::time::sleep(self.per_batch).await;
                batch.first_offset + 1
            }
            InternalMessage::ReplicateRecords(batch)
            | InternalMessage::ReplicateCacheRecords(batch) => {
                batch.first_offset + batch.payloads.len() as u64
            }
            other => panic!("unexpected {:?}", other.kind()),
        };
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset,
        }))
    }
}

/// **A long counter catch-up is reported a slice at a time.** Placement
/// drops a restore whose reported position stops moving, and for a cache used
/// only for counters the counter offset is that position. Shipped in one go,
/// it would only move once the whole catch-up was done.
#[tokio::test(start_paused = true)]
async fn a_counter_catch_up_is_reported_a_slice_at_a_time() {
    const PER_BATCH: Duration = Duration::from_secs(2);
    const ADDS: u64 = 8;
    let (broker, router, _dir) = quorum_cache().await;
    for _ in 0..ADDS {
        broker
            .counters()
            .expect("counters")
            .add(TENANT, NAMESPACE, STREAM, 0, "hits", 1)
            .await
            .expect("add");
    }
    let counters_tail = broker
        .shard_log(
            felix_broker::LogKind::Counters,
            TENANT,
            NAMESPACE,
            STREAM,
            0,
        )
        .await
        .expect("log")
        .tail_offset()
        .await
        .expect("tail");
    let requester = SlowCounters {
        per_batch: PER_BATCH,
    };
    let marks = QuorumMarks::new();
    let mut maps: [HashMap<ShardKey, ShardCursors>; 4] = Default::default();
    let mut reported = Vec::new();
    loop {
        let started = tokio::time::Instant::now();
        let [cursors, group, dead_letters, counters] = &mut maps;
        let pass = replicate_once(
            &requester,
            &broker,
            &router,
            &marks,
            None,
            cursors,
            group,
            dead_letters,
            counters,
        )
        .await;
        let took = started.elapsed();
        assert!(
            took <= CATCH_UP_SLICE + PER_BATCH,
            "one pass took {took:?}: it waited for the whole counter catch-up",
        );
        let offset = pass
            .reports
            .iter()
            .find(|report| report.key == cache_key())
            .and_then(|report| {
                report
                    .counter_offsets
                    .iter()
                    .find(|(node, _)| node == "broker-b")
                    .map(|(_, offset)| *offset)
            });
        reported.push(offset);
        if !pass.copying {
            break;
        }
        assert!(
            reported.len() <= ADDS as usize,
            "the catch-up never finished"
        );
    }
    assert!(reported.len() > 1, "caught up in one pass: {reported:?}");
    assert!(
        reported.windows(2).all(|pair| pair[0] < pair[1]),
        "every pass reported broker-b further on: {reported:?}",
    );
    assert_eq!(reported.last(), Some(&Some(counters_tail)));
}

/// **A counter write on this shard cuts a pass waiting on a majority.** The
/// cache log does not move, only the counter log: with b busy and d slow, the
/// pass waits on d, and the add must reach the free follower c now.
#[tokio::test(start_paused = true)]
async fn a_counter_write_on_this_shard_cuts_a_waiting_pass_short() {
    use super::stragglers::{Recording, SOON};
    let (broker, router, _dir) = quorum_cache().await;
    let nodes = nodes();
    router.publish(
        RoutingTable::build(
            [(
                cache_key(),
                LOCAL.to_string(),
                ["broker-b", "broker-c", "broker-d"]
                    .map(str::to_string)
                    .to_vec(),
                GENERATION,
            )],
            &nodes,
        ),
        &nodes,
    );
    let peers = Arc::new(Recording::default());
    let marks = by_followers(&broker).await;
    let (driver, marks) = self::driver(Arc::clone(&peers), &broker, &router, marks);
    let tails = write(&broker, 0).await;
    assert!(prompt(&marks, tails).await, "the first marks waited");

    // b stays busy; c and d make the majority.
    let rtt = Duration::from_millis(400);
    peers.delay("broker-b", rtt);
    let tails = write(&broker, 1).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(prompt(&marks, tails).await, "the majority waited");

    // Now d is slow and the next pass waits on it.
    peers.delay("broker-c", Duration::from_millis(20));
    peers.delay("broker-d", rtt);
    write(&broker, 2).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let landed = tokio::time::Instant::now();
    let adding = Arc::clone(&broker);
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        runtime
            .block_on(
                adding
                    .counters()
                    .expect("counters")
                    .add(TENANT, NAMESPACE, STREAM, 0, "hits", 1),
            )
            .expect("add");
    })
    .await
    .expect("add task");
    broker.appended().notify_one();
    tokio::time::sleep(SOON).await;
    assert!(
        peers.sent_between("broker-c", landed, tokio::time::Instant::now()),
        "the counter add waited for the pass to end before it was shipped to a free follower",
    );
    driver.stop().await;
}
