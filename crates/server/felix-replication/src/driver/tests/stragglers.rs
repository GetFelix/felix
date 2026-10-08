//! A slow peer holds nobody else's quorum mark.
//!
//! Each test runs the driver, lets one follower sit on its answer far longer
//! than any timeout, appends a record, and times the mark reaching it. The
//! clock is paused, so "well under a second" against a 60 s peer is exact.

use std::time::Duration;

use super::learner::publish_move;
use super::*;

/// How long the slow peer takes to answer anything.
const SLOW: Duration = Duration::from_secs(60);

/// How long a mark may take once there is a majority for it.
const PROMPT: Duration = Duration::from_millis(500);

/// Every request to `slow` takes [`SLOW`]; everyone else stores at once.
struct SlowPeer {
    slow: &'static str,
}

impl PeerRequester for SlowPeer {
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
            tokio::time::sleep(SLOW).await;
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }
}

/// Every request to a node waits that node's current delay, which a test can
/// change between appends; everyone else stores at once.
#[derive(Default)]
struct ShiftingPeer {
    delays: std::sync::Mutex<Map<&'static str, Duration>>,
}

impl ShiftingPeer {
    fn delay(&self, node: &'static str, by: Duration) {
        self.delays.lock().expect("lock").insert(node, by);
    }
}

impl PeerRequester for ShiftingPeer {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let InternalMessage::ReplicateRecords(batch) = message else {
            panic!("the driver sent something other than a replication batch");
        };
        let delay = self.delays.lock().expect("lock").get(node_id).copied();
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }
}

fn driver(
    slow: &'static str,
    broker: &Arc<Broker>,
    router: &Arc<ShardRouter>,
    marks: &Arc<QuorumMarks>,
    routes_changed: &Arc<tokio::sync::Notify>,
) -> Replication {
    driver_with(
        Arc::new(SlowPeer { slow }),
        broker,
        router,
        marks,
        routes_changed,
    )
}

fn driver_with<R: PeerRequester + Send + Sync + 'static>(
    requester: Arc<R>,
    broker: &Arc<Broker>,
    router: &Arc<ShardRouter>,
    marks: &Arc<QuorumMarks>,
    routes_changed: &Arc<tokio::sync::Notify>,
) -> Replication {
    spawn(
        requester,
        Arc::clone(broker),
        Arc::clone(router),
        Arc::new(Unfenced),
        Arc::new(crate::promotion::NoGate),
        Published {
            marks: Arc::clone(marks),
            halted: Arc::new(crate::halted::HaltedReplicas::new()),
            status: Arc::default(),
        },
        None,
        // Reaching the tick would mean nothing else woke the driver.
        Duration::from_secs(300),
        Arc::clone(routes_changed),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        CancellationToken::new(),
    )
}

/// Append one record to `shard` and wake the driver, as a publish does.
async fn append(broker: &Broker, shard: u32) {
    let log = broker
        .shard_log(
            felix_broker::LogKind::Stream,
            TENANT,
            NAMESPACE,
            STREAM,
            shard,
        )
        .await
        .expect("log");
    // The append waits on the log's own thread, and a paused runtime with
    // nothing to poll jumps its clock to the next timer meanwhile. Blocking
    // work holds the clock, so wait from there.
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        runtime
            .block_on(log.append(&[Bytes::from_static(b"late")]))
            .expect("append")
    })
    .await
    .expect("append task");
    broker.appended().notify_one();
}

/// Whether the mark for `shard` at `generation` reaches `offset` within
/// [`PROMPT`].
async fn prompt(marks: &QuorumMarks, shard: u32, generation: u64, offset: u64) -> bool {
    let watched = watch_key(&ShardKey { shard, ..key() });
    tokio::time::timeout(PROMPT, async {
        while marks.offset(&watched, generation) < Some(offset) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok()
}

/// **A destination that does not answer holds no later mark.** A stream with
/// one replica is moving to a broker that is paused. The destination does not
/// count toward the quorum, so the first mark comes at once -- and so must the
/// next one, while the exchange with the destination is still hanging.
#[tokio::test(start_paused = true)]
async fn a_destination_that_does_not_answer_holds_no_later_mark() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &[], 1);
    let marks = Arc::new(QuorumMarks::new());
    let routes_changed = Arc::new(tokio::sync::Notify::new());
    let driver = driver("broker-b", &broker, &router, &marks, &routes_changed);
    // A pass with no replica first, so the destination is known to be new.
    tokio::time::sleep(Duration::from_millis(50)).await;

    publish_move(&router, &["broker-b"], Some("broker-b"), 2);
    routes_changed.notify_one();
    assert!(prompt(&marks, 0, 2, 3).await, "the first mark waited");

    append(&broker, 0).await;
    assert!(
        prompt(&marks, 0, 2, 4).await,
        "the mark for a later record waited on the destination's exchange",
    );
    driver.stop().await;
}

/// **A follower beyond the majority holds no later mark.** Three replicas,
/// one of them unresponsive: the other follower is the majority, for the
/// record appended while the slow exchange is under way as for the first.
#[tokio::test(start_paused = true)]
async fn a_follower_beyond_the_majority_holds_no_later_mark() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let marks = Arc::new(QuorumMarks::new());
    let driver = driver("broker-c", &broker, &router, &marks, &Arc::default());
    assert!(prompt(&marks, 0, 4, 3).await, "the first mark waited");

    append(&broker, 0).await;
    assert!(
        prompt(&marks, 0, 4, 4).await,
        "the mark for a later record waited on the slow follower",
    );
    driver.stop().await;
}

/// **A busy follower's answer completes a majority the pass is waiting on.**
/// Follower b is still answering an earlier pass when a record lands, so the
/// next pass ships only to c, which has stopped answering. b's answer, which
/// covers the record, reaches the driver rather than that pass; the mark must
/// move on it instead of waiting out c.
#[tokio::test(start_paused = true)]
async fn a_busy_followers_answer_moves_a_mark_the_pass_waits_on() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let marks = Arc::new(QuorumMarks::new());
    let peers = Arc::new(ShiftingPeer::default());
    let driver = driver_with(
        Arc::clone(&peers),
        &broker,
        &router,
        &marks,
        &Arc::default(),
    );
    assert!(prompt(&marks, 0, 4, 3).await, "the first mark waited");

    // c makes the majority for this one, and b's exchange is still under way
    // when the next record lands.
    peers.delay("broker-b", Duration::from_secs(1));
    append(&broker, 0).await;
    assert!(
        prompt(&marks, 0, 4, 4).await,
        "c's answer did not move the mark"
    );

    peers.delay("broker-c", SLOW);
    append(&broker, 0).await;
    let watched = watch_key(&key());
    let reached = tokio::time::timeout(Duration::from_secs(10), async {
        while marks.offset(&watched, 4) < Some(5) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "the mark waited on the unresponsive follower while the busy one held the record",
    );
    driver.stop().await;
}

/// **One shard's slow follower holds no other shard's mark.** Shard 0 cannot
/// reach a majority while its only follower sits on an answer; shard 1's
/// follower answers at once, and its marks must not wait for shard 0's pass.
#[tokio::test(start_paused = true)]
async fn one_shards_slow_follower_holds_no_other_shards_mark() {
    let (broker, _dir) = leader_with_shards(2, 3).await;
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes = nodes();
    let table = RoutingTable::build(
        [
            (key(), LOCAL.to_string(), vec!["broker-b".to_string()], 4),
            (
                ShardKey { shard: 1, ..key() },
                LOCAL.to_string(),
                vec!["broker-c".to_string()],
                4,
            ),
        ],
        &nodes,
    );
    router.publish(table, &nodes);
    let marks = Arc::new(QuorumMarks::new());
    let driver = driver("broker-b", &broker, &router, &marks, &Arc::default());
    assert!(prompt(&marks, 1, 4, 3).await, "the first mark waited");

    append(&broker, 1).await;
    assert!(
        prompt(&marks, 1, 4, 4).await,
        "shard 1's mark waited on shard 0's follower",
    );
    driver.stop().await;
}

/// Offers the fence's capabilities; refuses the fence until `fence_ready`,
/// then takes it with an empty log. Stores every batch, and counts them.
#[derive(Default)]
struct FencingFollowers {
    fence_ready: std::sync::atomic::AtomicBool,
    shipped: std::sync::atomic::AtomicUsize,
}

impl PeerRequester for FencingFollowers {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        use std::sync::atomic::Ordering;
        match message {
            InternalMessage::Fence(fence) => {
                if !self.fence_ready.load(Ordering::SeqCst) {
                    return Err(PeerError::Unavailable {
                        node_id: node_id.to_string(),
                        detail: "not yet".to_string(),
                    });
                }
                Ok(InternalMessage::FenceOk(felix_wire::internal::FenceOk {
                    correlation_id: fence.correlation_id,
                    log_end: 0,
                    commit_offset: 0,
                    last_generation: 0,
                }))
            }
            InternalMessage::ReplicateRecords(batch) => {
                self.shipped.fetch_add(1, Ordering::SeqCst);
                Ok(InternalMessage::ReplicateOk(ReplicateOk {
                    correlation_id: 0,
                    durable_offset: batch.first_offset + batch.payloads.len() as u64,
                }))
            }
            other => panic!("unexpected {:?}", other.kind()),
        }
    }

    async fn capabilities(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
    ) -> std::result::Result<felix_wire::internal::PeerCapabilities, PeerError> {
        Ok(crate::promotion::REQUIRED)
    }
}

/// Promoted at a generation, until opened.
struct Awaiting {
    generation: Mutex<Option<u64>>,
}

#[async_trait::async_trait]
impl crate::promotion::PromotionGate for Awaiting {
    fn awaiting(&self, _key: &crate::ShardKey) -> Option<u64> {
        *self.generation.lock().expect("lock")
    }

    async fn open(&self, _key: &crate::ShardKey, _generation: u64) -> bool {
        *self.generation.lock().expect("lock") = None;
        true
    }
}

/// **A promoted shard ships nothing and moves no mark before its fence.**
/// Passing each shard on its own must not let one slip past the gate: the
/// fence may yet take a tail that shipping would have truncated, and a mark
/// before it would count acknowledgements from before the promotion.
#[tokio::test(start_paused = true)]
async fn a_promoted_shard_ships_nothing_before_its_fence() {
    use std::sync::atomic::Ordering;

    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let marks = Arc::new(QuorumMarks::new());
    let followers = Arc::new(FencingFollowers::default());
    let gate = Arc::new(Awaiting {
        generation: Mutex::new(Some(4)),
    });
    let status = Arc::new(crate::status::ShardStatusBoard::new());
    let driver = spawn(
        Arc::clone(&followers),
        Arc::clone(&broker),
        Arc::clone(&router),
        Arc::new(Unfenced),
        Arc::clone(&gate) as Arc<dyn crate::promotion::PromotionGate>,
        Published {
            marks: Arc::clone(&marks),
            halted: Arc::new(crate::halted::HaltedReplicas::new()),
            status: Arc::clone(&status),
        },
        None,
        Duration::from_secs(300),
        Arc::default(),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        CancellationToken::new(),
    );

    // Appends and retries while the fence is refused.
    for _ in 0..5 {
        append(&broker, 0).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert_eq!(
        followers.shipped.load(Ordering::SeqCst),
        0,
        "shipped before the fence"
    );
    assert_eq!(
        marks.offset(&watch_key(&key()), 4),
        None,
        "a mark moved before the fence",
    );
    // What an operator inspecting the shard reads while it waits.
    let fencing = status.get(&watch_key(&key())).expect("on the board");
    let fence = fencing.fence.expect("fencing");
    assert!(fence.took.is_empty(), "{fence:?}");
    assert_eq!(fence.pending, ["broker-b", "broker-c"]);
    assert!(fence.attempts >= 1, "{fence:?}");
    assert!(fencing.followers.is_empty());

    followers.fence_ready.store(true, Ordering::SeqCst);
    let opened = tokio::time::timeout(Duration::from_secs(5), async {
        while marks.offset(&watch_key(&key()), 4) < Some(8) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(opened.is_ok(), "the shard never shipped once fenced");
    assert!(gate.awaiting(&watch_key(&key())).is_none());
    let shipping = status.get(&watch_key(&key())).expect("on the board");
    assert_eq!(shipping.fence, None);
    assert_eq!(shipping.generation, 4);
    assert_eq!(shipping.followers.len(), 2, "{shipping:?}");
    driver.stop().await;
}

/// Fenced every time, and kept closed every time, as the broker does when it
/// cannot write the shard's generation-start record.
struct KeptClosed {
    opens: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl crate::promotion::PromotionGate for KeptClosed {
    fn awaiting(&self, _key: &crate::ShardKey) -> Option<u64> {
        Some(4)
    }

    async fn open(&self, _key: &crate::ShardKey, _generation: u64) -> bool {
        self.opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        false
    }
}

/// **A shard the broker keeps closed after its fence backs off** rather than
/// fencing again every [`FENCE_RETRY`] for as long as the failure lasts, and
/// appends to other shards do not hurry it.
#[tokio::test(start_paused = true)]
async fn a_shard_kept_closed_after_its_fence_backs_off() {
    use std::sync::atomic::Ordering;

    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let followers = Arc::new(FencingFollowers::default());
    followers.fence_ready.store(true, Ordering::SeqCst);
    let gate = Arc::new(KeptClosed {
        opens: Default::default(),
    });
    let driver = spawn(
        Arc::clone(&followers),
        Arc::clone(&broker),
        Arc::clone(&router),
        Arc::new(Unfenced),
        Arc::clone(&gate) as Arc<dyn crate::promotion::PromotionGate>,
        Published {
            marks: Arc::new(QuorumMarks::new()),
            halted: Arc::new(crate::halted::HaltedReplicas::new()),
            status: Arc::default(),
        },
        None,
        Duration::from_secs(300),
        Arc::default(),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        CancellationToken::new(),
    );

    // 20 s of appends: every 200 ms would be 100 attempts. Backed off it is
    // 0, 0.2, 0.6, 1.4, 3.0, then every 2 s.
    for _ in 0..200 {
        append(&broker, 0).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let opens = gate.opens.load(Ordering::SeqCst);
    assert!((10..=20).contains(&opens), "{opens} attempts in 20 s");
    driver.stop().await;
}

#[test]
fn the_fence_backoff_doubles_to_its_cap() {
    let waits: Vec<_> = (1..=7).map(fence_backoff).collect();
    assert_eq!(waits[0], FENCE_RETRY);
    assert_eq!(waits[1], FENCE_RETRY * 2);
    assert_eq!(waits[3], FENCE_RETRY * 8);
    assert_eq!(waits[6], FENCE_RETRY_MAX);
    assert_eq!(fence_backoff(u32::MAX), FENCE_RETRY_MAX);
}
