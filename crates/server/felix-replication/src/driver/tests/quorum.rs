//! The quorum mark: how far a majority of the replica set holds the shard.

use super::*;

/// Answers at once for everyone but one node, which it keeps waiting.
struct OneSlowFollower {
    slow: &'static str,
    delay: Duration,
}

impl PeerRequester for OneSlowFollower {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let InternalMessage::ReplicateRecords(batch) = message else {
            panic!("the driver sent something other than a replication batch");
        };
        if node_id == self.slow {
            tokio::time::sleep(self.delay).await;
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }
}

/// **The mark advances at the majority, not at the last follower.**
///
/// With three replicas a record is on a majority the moment one follower has
/// it; the second is a durability margin, not a precondition. Waiting for every
/// follower put one dead or slow replica's whole timeout in front of every
/// acknowledgement on the shard, every pass — the failure `Quorum` exists to
/// tolerate, turned into the thing that stalls it (#411).
///
/// The clock is the assertion: the mark has to reach the tail long before the
/// slow follower answers at all.
#[tokio::test(start_paused = true)]
async fn the_mark_advances_at_the_majority_not_at_the_slowest_follower() {
    const SLOW: Duration = Duration::from_secs(60);
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let follower = OneSlowFollower {
        slow: "broker-c",
        delay: SLOW,
    };
    let marks = QuorumMarks::new();
    let watched = watch_key(&key());
    let started = tokio::time::Instant::now();

    let observe = async {
        loop {
            if marks.offset(&watched, 4) == Some(3) {
                return tokio::time::Instant::now().duration_since(started);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };

    let (mut cursors, mut group, mut dead, mut counters) = (
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    );
    let (_, reached) = tokio::join!(
        replicate_once(
            &follower,
            &broker,
            &router,
            &marks,
            None,
            &mut cursors,
            &mut group,
            &mut dead,
            &mut counters,
        ),
        observe,
    );

    assert!(
        reached < SLOW,
        "the majority held every record after {reached:?}, but the mark waited \
         {SLOW:?} for the slowest follower",
    );
}

/// The slow follower is still shipped to and still ends the pass at the tail.
/// Publishing at the majority is about when the mark moves, not about giving up
/// on the rest of the replica set.
#[tokio::test(start_paused = true)]
async fn the_slower_follower_is_still_caught_up_by_the_end_of_the_pass() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let follower = OneSlowFollower {
        slow: "broker-c",
        delay: Duration::from_secs(60),
    };
    let marks = QuorumMarks::new();

    let pass = replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    let mut caught_up = pass.reports[0].caught_up.clone();
    caught_up.sort();
    assert_eq!(
        caught_up,
        vec!["broker-b".to_string(), "broker-c".to_string()],
        "the slow follower was abandoned rather than waited for",
    );
    let offsets: Map<String, u64> = pass.reports[0].offsets.iter().cloned().collect();
    assert_eq!(offsets.get("broker-c"), Some(&3));
}

/// **A record the only follower disagrees with is never acknowledged.**
///
/// This is the end of the chain #406 described. A follower keeps an orphan from
/// a dead leader; the new leader reuses that offset for its own record; and if
/// the mark moved past it anyway, `Quorum` would acknowledge a record no
/// majority holds — and the follower, looking level, would then be promoted
/// over it.
///
/// The two links before this one are checked next door, on a real follower log:
/// `a_batch_wholly_overlapping_does_not_confirm_past_itself` and
/// `an_orphan_at_a_reused_offset_is_reported_as_a_conflict` in
/// `felix-broker`. This is the part that decides whether a client is told yes.
#[tokio::test]
async fn a_quorum_mark_does_not_pass_a_record_the_follower_disagrees_with() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let marks = QuorumMarks::new();

    replicate_once(
        &DivergingFollower,
        &broker,
        &router,
        &marks,
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    // What holds here is that the mark is derived from where followers actually
    // are: this one never got past its first batch, so no majority reaches the
    // leader's tail and there is nothing to mark. A mark taken from the
    // leader's own tail instead fails this with "reached Some(3)", which is
    // what acknowledging on the leader's word alone would look like.
    let mark = marks.offset(&watch_key(&key()), 4);
    assert!(
        mark.is_none_or(|offset| offset < 3),
        "the quorum mark reached {mark:?} with the only follower in \
         disagreement, so a record no majority holds was acknowledged",
    );
}

/// Followers that store a batch only if it ends by `held`, and are
/// unreachable for any other.
struct HoldUpTo {
    held: std::sync::atomic::AtomicU64,
}

impl PeerRequester for HoldUpTo {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let (InternalMessage::ReplicateRecords(batch)
        | InternalMessage::ReplicateMarkedRecords(batch)) = message
        else {
            panic!("the driver sent something other than a replication batch");
        };
        let end = batch.first_offset + batch.payloads.len() as u64;
        if end > self.held.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "not yet".to_string(),
            });
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: end,
        }))
    }
}

/// Marks under the counting rule a fleet uses once it finalized
/// `generation_start`.
fn finalized_marks() -> QuorumMarks {
    let fleet = felix_common::fleet::FleetGate::new([felix_common::fleet::GENERATION_START.name()]);
    fleet.observe([felix_common::fleet::GENERATION_START.name()]);
    QuorumMarks::with_fleet(Arc::new(fleet))
}

/// **A promoted leader's mark covers what it inherited only once a majority
/// holds its generation-start record.** Counting the inherited records on
/// their own is Raft's Figure 8: a later fence may prefer a log whose last
/// generation is newer and drop them after they were acknowledged.
#[tokio::test]
async fn inherited_records_count_only_once_the_start_record_is_on_a_majority() {
    let (broker, _dir) = leader_led_from(3, 4, 3).await;
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    assert_eq!(
        log.append_generation_start(4).await.expect("start record"),
        3
    );
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let followers = HoldUpTo {
        held: std::sync::atomic::AtomicU64::new(3),
    };
    let marks = finalized_marks();
    let watched = watch_key(&key());
    let (mut cursors, mut group, mut dead, mut counters) = (
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    );

    replicate_once(
        &followers,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut group,
        &mut dead,
        &mut counters,
    )
    .await;
    assert_eq!(
        marks.offset(&watched, 4).unwrap_or(0),
        0,
        "the mark counted records the leader inherited before its own was on a majority",
    );

    followers.held.store(4, std::sync::atomic::Ordering::SeqCst);
    replicate_once(
        &followers,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut group,
        &mut dead,
        &mut counters,
    )
    .await;
    assert_eq!(marks.offset(&watched, 4), Some(4));
}

/// **Once finalized, a leader with no start record still counts nothing it
/// inherited.** Whatever path named it, the first record of its own
/// generation on a majority is what moves the mark, here a client's.
#[tokio::test]
async fn a_finalized_leader_without_a_start_record_counts_only_its_own_records() {
    let (broker, _dir) = leader_led_from(3, 4, 3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let followers = HoldUpTo {
        held: std::sync::atomic::AtomicU64::new(3),
    };
    let marks = finalized_marks();
    let watched = watch_key(&key());
    let (mut cursors, mut group, mut dead, mut counters) = (
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    );

    replicate_once(
        &followers,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut group,
        &mut dead,
        &mut counters,
    )
    .await;
    assert_eq!(marks.offset(&watched, 4).unwrap_or(0), 0);

    broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .append(&[Bytes::from_static(b"own")])
        .await
        .expect("append");
    followers.held.store(4, std::sync::atomic::Ordering::SeqCst);
    replicate_once(
        &followers,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut group,
        &mut dead,
        &mut counters,
    )
    .await;
    assert_eq!(marks.offset(&watched, 4), Some(4));
}

/// Before the fleet finalizes `generation_start` the leader counts the
/// records it inherited, as an older build does, so a rollback changes
/// nothing.
#[tokio::test]
async fn before_finalize_inherited_records_count_as_before() {
    let (broker, _dir) = leader_led_from(3, 4, 3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let followers = HoldUpTo {
        held: std::sync::atomic::AtomicU64::new(3),
    };
    let marks = QuorumMarks::new();

    replicate_once(
        &followers,
        &broker,
        &router,
        &marks,
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
    assert_eq!(marks.offset(&watch_key(&key()), 4), Some(3));
}

/// Marks as a fleet that finalized both `generation_start` and
/// `majority_ack` counts them: by the followers.
fn follower_ack_marks() -> QuorumMarks {
    use felix_common::fleet::{FleetGate, GENERATION_START, MAJORITY_ACK};
    let names = [GENERATION_START.name(), MAJORITY_ACK.name()];
    let fleet = FleetGate::new(names);
    fleet.observe(names);
    QuorumMarks::with_fleet(Arc::new(fleet))
}

/// A reporter whose control plane is gone: every report fails to land.
fn unreachable_reporter() -> (
    crate::reporter::Reporter,
    tokio_util::sync::CancellationToken,
) {
    let shutdown = tokio_util::sync::CancellationToken::new();
    let (reporter, _task) = crate::reporter::Reporter::spawn(
        crate::reporter::ReportTo {
            client: reqwest::Client::new(),
            base_url: "http://127.0.0.1:1".to_string(),
            node_id: LOCAL.to_string(),
            token: None,
            incarnation: 1,
        },
        shutdown.clone(),
    );
    (reporter, shutdown)
}

/// Stores every batch on `broker-b`; `broker-c` is unreachable.
struct OnlyB;

impl PeerRequester for OnlyB {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let (InternalMessage::ReplicateRecords(batch)
        | InternalMessage::ReplicateMarkedRecords(batch)) = message
        else {
            panic!("the driver sent something other than a replication batch");
        };
        if node_id != "broker-b" {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "partitioned".to_string(),
            });
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }
}

/// [`leader_led_from`], with the stream registered as `Quorum`.
async fn quorum_leader(count: usize, generation: u64) -> (Arc<Broker>, tempfile::TempDir) {
    let (broker, dir) = leader_led_from(count, generation, 0).await;
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
                shards: 1,
                consistency: felix_broker::ConsistencyLevel::Quorum,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    (broker, dir)
}

async fn pass_with_reporter<R: PeerRequester + Sync>(
    followers: &R,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    marks: &QuorumMarks,
    reporter: &crate::reporter::Reporter,
) {
    let (mut cursors, mut group, mut dead, mut counters) = (
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    );
    replicate_once(
        followers,
        broker,
        router,
        marks,
        Some(reporter),
        &mut cursors,
        &mut group,
        &mut dead,
        &mut counters,
    )
    .await;
}

/// **With follower acks the mark moves on a majority's answers, whether or
/// not the control plane heard the report.** Under the report rule the same
/// pass withholds it.
#[tokio::test]
async fn follower_acks_move_the_mark_without_the_report() {
    let (reporter, shutdown) = unreachable_reporter();
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let watched = watch_key(&key());

    let (broker, _dir) = quorum_leader(3, 4).await;
    let marks = follower_ack_marks();
    pass_with_reporter(&OnlyB, &broker, &router, &marks, &reporter).await;
    assert_eq!(marks.offset(&watched, 4), Some(3));
    assert!(marks.decided_by_followers(&watched, 4));

    let (broker, _dir) = quorum_leader(3, 4).await;
    let marks = finalized_marks();
    pass_with_reporter(&OnlyB, &broker, &router, &marks, &reporter).await;
    assert_eq!(
        marks.offset(&watched, 4).unwrap_or(0),
        0,
        "the report rule released a mark the control plane never heard of",
    );
    shutdown.cancel();
}

/// **A leader that took a newer leader's fence does not count itself.** Its
/// one follower is not a majority of three, so nothing it holds is
/// acknowledged.
#[tokio::test]
async fn a_fenced_leader_does_not_count_its_own_copy() {
    let (reporter, shutdown) = unreachable_reporter();
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let watched = watch_key(&key());
    let (broker, _dir) = quorum_leader(3, 4).await;
    broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .accept_generation(5)
        .await
        .expect("accept a newer leader");
    let marks = follower_ack_marks();

    pass_with_reporter(&OnlyB, &broker, &router, &marks, &reporter).await;

    assert_eq!(
        marks.offset(&watched, 4).unwrap_or(0),
        0,
        "a deposed leader counted itself toward a majority",
    );
    shutdown.cancel();
}

/// Stores every cache and counter batch on `broker-b`; `broker-c` is
/// unreachable.
struct CacheOnlyB;

impl PeerRequester for CacheOnlyB {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let (InternalMessage::ReplicateCacheRecords(batch)
        | InternalMessage::ReplicateCounterRecords(batch)) = message
        else {
            panic!("the driver sent {:?} for a cache shard", message.kind());
        };
        if node_id != "broker-b" {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "partitioned".to_string(),
            });
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }
}

/// A `Quorum` cache led here at `generation` since offset zero, with two
/// records in its cache log and three in its counter log.
async fn quorum_cache_leader(generation: u64) -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let broker = Arc::new(
        Broker::new(Box::new(
            felix_storage::LogCache::open(dir.path().join("caches"), config.clone())
                .expect("cache"),
        ))
        // Replication runs only on a broker with durable storage.
        .with_durable_storage(
            DurableStorage::open(dir.path().join("streams"), config.clone()).expect("storage"),
        )
        .with_counters(Arc::new(
            felix_storage::CounterStore::open(dir.path().join("counters"), config)
                .expect("counters"),
        )),
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
            },
        )
        .await
        .expect("cache");
    for (kind, count) in [
        (felix_broker::LogKind::Cache, 2),
        (felix_broker::LogKind::Counters, 3),
    ] {
        let log = broker
            .shard_log(kind, TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("log");
        log.record_generation(generation, 0).expect("generation");
        let payloads: Vec<Bytes> = (0..count).map(|i| Bytes::from(format!("r{i}"))).collect();
        log.append(&payloads).await.expect("append");
    }
    (broker, dir)
}

/// Marks as a fleet that finalized `generation_start` and `majority_ack`,
/// and `fenced_caches` when `caches` says so.
fn fleet_marks(caches: bool) -> QuorumMarks {
    use felix_common::fleet::{FENCED_CACHES, FleetGate, GENERATION_START, MAJORITY_ACK};
    let mut names = vec![GENERATION_START.name(), MAJORITY_ACK.name()];
    if caches {
        names.push(FENCED_CACHES.name());
    }
    let fleet = FleetGate::new(names.clone());
    fleet.observe(names);
    QuorumMarks::with_fleet(Arc::new(fleet))
}

/// **A `Quorum` cache's mark and its counter mark move on a majority's
/// answers once the fleet fences caches**, whether or not the control plane
/// heard the report. Without `fenced_caches` the same pass withholds both,
/// as it did before: an older broker could still be promoted unfenced.
#[tokio::test]
async fn a_fenced_caches_fleet_acknowledges_cache_writes_by_the_followers() {
    let (reporter, shutdown) = unreachable_reporter();
    let cache = ShardKey {
        kind: felix_router::ShardKind::Cache,
        ..key()
    };
    let watched = watch_key(&cache);
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes = nodes();
    router.publish(
        RoutingTable::build(
            [(
                cache,
                LOCAL.to_string(),
                vec!["broker-b".to_string(), "broker-c".to_string()],
                4,
            )],
            &nodes,
        ),
        &nodes,
    );

    let (broker, _dir) = quorum_cache_leader(4).await;
    let marks = fleet_marks(true);
    pass_with_reporter(&CacheOnlyB, &broker, &router, &marks, &reporter).await;
    assert_eq!(marks.offset(&watched, 4), Some(2));
    assert!(marks.decided_by_followers(&watched, 4));
    assert_eq!(marks.counters().offset(&watched, 4), Some(3));
    assert!(marks.counters().decided_by_followers(&watched, 4));

    let (broker, _dir) = quorum_cache_leader(4).await;
    let marks = fleet_marks(false);
    pass_with_reporter(&CacheOnlyB, &broker, &router, &marks, &reporter).await;
    assert_eq!(marks.offset(&watched, 4), None);
    assert_eq!(marks.counters().offset(&watched, 4), None);

    shutdown.cancel();
}

/// **A cache leader counts only past where its generation begins**, as a
/// stream leader does, once the fleet finalized `generation_start`: a
/// majority holding only what it inherited moves neither mark.
#[tokio::test]
async fn a_cache_leader_counts_only_its_own_generation() {
    let (reporter, shutdown) = unreachable_reporter();
    let cache = ShardKey {
        kind: felix_router::ShardKind::Cache,
        ..key()
    };
    let watched = watch_key(&cache);
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes = nodes();
    router.publish(
        RoutingTable::build(
            [(
                cache,
                LOCAL.to_string(),
                vec!["broker-b".to_string(), "broker-c".to_string()],
                5,
            )],
            &nodes,
        ),
        &nodes,
    );
    // Led at 4; generation 5 begins at each log's tail, with nothing of its
    // own yet.
    let (broker, _dir) = quorum_cache_leader(4).await;
    for kind in [
        felix_broker::LogKind::Cache,
        felix_broker::LogKind::Counters,
    ] {
        let log = broker
            .shard_log(kind, TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("log");
        let tail = log.tail_offset().await.expect("tail");
        log.record_generation(5, tail).expect("generation");
    }
    let marks = fleet_marks(true);
    pass_with_reporter(&CacheOnlyB, &broker, &router, &marks, &reporter).await;
    assert_eq!(marks.offset(&watched, 5).unwrap_or(0), 0);
    assert_eq!(marks.counters().offset(&watched, 5).unwrap_or(0), 0);

    shutdown.cancel();
}
