//! What the leader tells the control plane about its replicas.

use super::*;
use crate::status::ShardStatusBoard;
use felix_common::membership::RefusedGeneration;

/// A pass reports the shard, its generation, and who could take it over.
#[tokio::test]
async fn a_pass_reports_who_could_take_the_shard_over() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    let pass = replicate_once(
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

    assert_eq!(pass.reports.len(), 1);
    let report = &pass.reports[0];
    assert_eq!(report.key, key());
    assert_eq!(report.generation, 4);
    assert_eq!(report.caught_up, vec!["broker-b".to_string()]);
    // What the offsets are measured against: placement fences a move on how
    // far behind the destination is.
    assert_eq!(report.tail, 3);
}

/// **A follower of an empty shard is reported level with it.** Nothing is
/// shipped, so the leader never hears from it; without its offset in the
/// report, placement cannot seat it as a new copy, and the restore holds the
/// cluster's move slot until it times out (#1133).
#[tokio::test]
async fn a_follower_of_an_empty_shard_is_reported_level() {
    let (broker, _dir) = leader_with(0).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    let pass = replicate_once(
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

    assert_eq!(pass.reports.len(), 1);
    let report = &pass.reports[0];
    assert_eq!(report.tail, 0);
    assert_eq!(report.offsets, vec![("broker-b".to_string(), 0)]);
}

/// **A follower that did not keep up is not reported as able to lead.**
/// This is the whole point of the signal: the control plane promotes on it,
/// and a follower named here while behind would be promoted into a shard it
/// cannot serve.
#[tokio::test]
async fn a_follower_that_refused_is_not_reported_as_able_to_lead() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = RefusingFollower;
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    let pass = replicate_once(
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

    assert_eq!(pass.reports.len(), 1);
    assert!(
        pass.reports[0].caught_up.is_empty(),
        "a follower holding nothing was reported as able to lead",
    );
}

/// A shard this broker only follows is not reported on. Reporting about a
/// shard it does not lead would be an opinion it has no basis for.
#[tokio::test]
async fn a_shard_led_elsewhere_is_not_reported() {
    let (broker, _dir) = leader_with(3).await;
    let router = router("broker-b", &[LOCAL], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    let pass = replicate_once(
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

    assert!(pass.reports.is_empty());
}

/// **A shard led alone still reports.** It names nobody, but placement adds a
/// copy only once the leader has reported at the shard's generation, so a
/// shard placed while its leader was the only live broker stayed at one copy
/// for good (#1153).
#[tokio::test]
async fn a_shard_led_alone_reports_its_generation() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &[], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    let pass = replicate_once(
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

    assert_eq!(pass.reports.len(), 1);
    let report = &pass.reports[0];
    assert_eq!(report.key, key());
    assert_eq!(report.generation, 4);
    assert!(report.caught_up.is_empty());
    assert!(report.offsets.is_empty());
    assert_eq!(report.tail, 3);
    assert!(follower.batches().is_empty());
}

/// The running driver reports a shard led alone too, at generation 0 as a
/// first placement leaves it, and with an empty log.
#[tokio::test]
async fn the_driver_reports_a_shard_led_alone() {
    let (broker, _dir) = leader_with(0).await;
    let (control_plane, received) = recording_control_plane().await;
    let shutdown = CancellationToken::new();
    let driver = spawn(
        Arc::new(AcceptingFollower::default()),
        broker,
        router(LOCAL, &[], 0),
        Arc::new(Unfenced),
        Arc::new(crate::promotion::NoGate),
        published(),
        Some(control_plane.reporter(shutdown.clone())),
        Duration::from_millis(100),
        Arc::default(),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        shutdown.clone(),
    );

    let report = first_report(&received, Duration::from_secs(5)).await;
    shutdown.cancel();
    driver.stop().await;
    let report = report.expect("a shard led alone was never reported");
    assert_eq!((report.stream.as_str(), report.generation), (STREAM, 0));
    assert!(report.caught_up.is_empty());
    assert!(report.replica_offsets.is_empty());
    assert_eq!(report.leader_offset, Some(0));
}

/// A shard led alone keeps its reported tail as a floor, but only for the
/// same log: a stream recreated at the deleted one's generation (after a
/// restore rewinds the change seq) starts empty, and reporting the old tail
/// for it would claim records the new log does not have.
#[tokio::test]
async fn a_shard_led_alone_drops_its_floor_for_a_new_incarnation() {
    let (broker, _dir) = incarnated_leader(5, 10).await;
    let (control_plane, received) = recording_control_plane().await;
    let shutdown = CancellationToken::new();
    let driver = spawn(
        Arc::new(AcceptingFollower::default()),
        Arc::clone(&broker),
        router(LOCAL, &[], 4),
        Arc::new(Unfenced),
        Arc::new(crate::promotion::NoGate),
        published(),
        Some(control_plane.reporter(shutdown.clone())),
        Duration::from_millis(50),
        Arc::default(),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        shutdown.clone(),
    );
    let of = |incarnation: u64| {
        let received = Arc::clone(&received);
        async move {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let found = received
                        .lock()
                        .expect("lock")
                        .iter()
                        .find(|report| report.stream_incarnation == incarnation)
                        .cloned();
                    if let Some(report) = found {
                        return report;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .ok()
        }
    };

    let old = of(5)
        .await
        .expect("the first incarnation was never reported");
    assert_eq!(old.leader_offset, Some(10));
    reincarnate(&broker, 6, 0).await;
    let new = of(6).await;
    shutdown.cancel();
    driver.stop().await;
    let new = new.expect("the new incarnation was never reported");
    assert_eq!(
        (new.generation, new.leader_offset),
        (4, Some(0)),
        "the new log was reported at the old log's tail",
    );
}

/// **A promoted shard led alone reports nothing until its fence opens it.**
/// The report is what lets placement grow the set, and a set written while
/// the leader is still fencing would be the set it fences.
#[tokio::test]
async fn a_promoted_shard_led_alone_reports_only_once_open() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Gate {
        closed: AtomicBool,
    }

    #[async_trait::async_trait]
    impl crate::promotion::PromotionGate for Gate {
        fn awaiting(&self, _key: &crate::ShardKey) -> Option<u64> {
            self.closed.load(Ordering::SeqCst).then_some(4)
        }

        async fn open(&self, _key: &crate::ShardKey, _generation: u64) -> bool {
            !self.closed.load(Ordering::SeqCst)
        }
    }

    let (broker, _dir) = leader_with(3).await;
    let (control_plane, received) = recording_control_plane().await;
    let gate = Arc::new(Gate {
        closed: AtomicBool::new(true),
    });
    let shutdown = CancellationToken::new();
    let driver = spawn(
        Arc::new(AcceptingFollower::default()),
        broker,
        router(LOCAL, &[], 4),
        Arc::new(Unfenced),
        Arc::clone(&gate) as Arc<dyn crate::promotion::PromotionGate>,
        published(),
        Some(control_plane.reporter(shutdown.clone())),
        Duration::from_millis(50),
        Arc::default(),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        shutdown.clone(),
    );

    let early = first_report(&received, Duration::from_millis(500)).await;
    gate.closed.store(false, Ordering::SeqCst);
    let opened = first_report(&received, Duration::from_secs(5)).await;
    shutdown.cancel();
    driver.stop().await;
    assert!(
        early.is_none(),
        "reported while the fence was still pending: {early:?}"
    );
    let opened = opened.expect("never reported once open");
    assert_eq!((opened.generation, opened.leader_offset), (4, Some(3)));
}

fn published() -> Published {
    Published {
        marks: Arc::new(QuorumMarks::new()),
        halted: Arc::new(crate::halted::HaltedReplicas::new()),
        status: Arc::default(),
    }
}

/// A control plane that stores every report it is sent.
struct RecordingControlPlane {
    addr: SocketAddr,
}

impl RecordingControlPlane {
    fn reporter(&self, shutdown: CancellationToken) -> crate::reporter::Reporter {
        crate::reporter::Reporter::spawn(
            ReportTo {
                client: reqwest::Client::new(),
                base_url: format!("http://{}", self.addr),
                node_id: LOCAL.to_string(),
                token: None,
                incarnation: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
                storage_id: None,
            },
            shutdown,
        )
        .0
    }
}

async fn recording_control_plane() -> (RecordingControlPlane, Arc<Mutex<Vec<ShardReplicaStatus>>>) {
    let received: Arc<Mutex<Vec<ShardReplicaStatus>>> = Arc::default();
    let sink = Arc::clone(&received);
    let app = axum::Router::new().route(
        "/v1/nodes/{node_id}/replica-status",
        axum::routing::post(
            move |axum::Json(request): axum::Json<ReplicaStatusRequest>| async move {
                sink.lock().expect("lock").extend(request.shards);
                axum::http::StatusCode::NO_CONTENT
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });
    (RecordingControlPlane { addr }, received)
}

/// The first report received within `within`.
async fn first_report(
    received: &Mutex<Vec<ShardReplicaStatus>>,
    within: Duration,
) -> Option<ShardReplicaStatus> {
    tokio::time::timeout(within, async {
        loop {
            if let Some(report) = received.lock().expect("lock").first().cloned() {
                return report;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .ok()
}

/// A follower that refuses everything, so it never advances.
struct RefusingFollower;

impl PeerRequester for RefusingFollower {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        _message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        Err(PeerError::Unavailable {
            node_id: "broker-b".to_string(),
            detail: "not now".to_string(),
        })
    }
}

/// A quorum mark is not published when the replica report did not land.
///
/// The mark is what releases a `Quorum` publish, and promotion reads the
/// report. Releasing on a report that never arrived is the same window the
/// report-then-publish ordering exists to close, reached by a different route:
/// a client is told its record is on a majority while the control plane knows
/// nothing about which replica holds it.
#[tokio::test]
async fn a_failed_replica_report_holds_the_quorum_mark_back() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    // A control plane that refuses every report.
    let app = axum::Router::new().route(
        "/v1/nodes/{node_id}/replica-status",
        axum::routing::post(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });

    let report_shutdown = CancellationToken::new();
    let (reporter, _reporter_task) = crate::reporter::Reporter::spawn(
        ReportTo {
            client: reqwest::Client::new(),
            base_url: format!("http://{addr}"),
            node_id: LOCAL.to_string(),
            token: None,
            incarnation: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            storage_id: None,
        },
        report_shutdown.clone(),
    );

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        Some(&reporter),
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    // Replication itself succeeded — the follower took every record — so this
    // is specifically about the report, not about shipping.
    assert_eq!(
        follower.batches().len(),
        1,
        "the records should still have been shipped: {:?}",
        follower.batches(),
    );
    // TimedOut, not Reached: the publish waits and the client is told a
    // timeout, which is the honest answer when this broker cannot make the
    // acknowledgement good at failover.
    assert!(
        matches!(
            marks
                .wait_for(&watch_key(&key()), 4, 1, Duration::from_millis(50))
                .await,
            crate::quorum::QuorumWait::TimedOut | crate::quorum::QuorumWait::NotLeading
        ),
        "the mark was published on a report the control plane never took, so a \
         client would be told its record is on a majority the control plane \
         cannot find at failover",
    );

    server.abort();
}

/// The report a broker sends parses as the type the control plane reads.
///
/// That is the whole point of sharing the definition rather than building the
/// body with `json!`: a field renamed on one side used to arrive at the other
/// as a missing one, with nothing failing to compile and nothing failing at
/// runtime until promotion went looking for a replica it could not find.
#[test]
fn a_report_body_is_the_shape_the_control_plane_parses() {
    let sent = ReplicaStatusRequest {
        incarnation: 2,
        shards: vec![ShardReplicaStatus {
            given_up: false,
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 3,
            kind: WireShardKind::Cache,
            generation: 9,
            caught_up: vec!["broker-b".to_string()],
            replica_offsets: vec![ReplicaOffset {
                node_id: "broker-b".to_string(),
                durable_offset: 41,
            }],
            drained: false,
            leader_offset: Some(44),
            halted: Vec::new(),
            replica_counter_offsets: vec![ReplicaOffset {
                node_id: "broker-b".to_string(),
                durable_offset: 7,
            }],
            stream_incarnation: 0,
            by_round: true,
            replica_incarnations: Default::default(),
            replica_storage_ids: Default::default(),
            inherited_storage_ids: Default::default(),
            joint_start_acked: None,
            refused: None,
            holds_start: Default::default(),
            elected: None,
        }],
    };

    let json = serde_json::to_value(&sent).expect("serialise");
    // The field names the control plane's handler reads, spelled out rather
    // than derived, so a rename has to be made deliberately here too.
    let shard = &json["shards"][0];
    assert_eq!(json["incarnation"], 2);
    assert_eq!(shard["tenant_id"], "t1");
    assert_eq!(shard["namespace"], "ns");
    assert_eq!(shard["stream"], "orders");
    assert_eq!(shard["shard"], 3);
    assert_eq!(shard["kind"], "cache");
    assert_eq!(shard["generation"], 9);
    assert_eq!(shard["by_round"], true);
    assert_eq!(shard["caught_up"][0], "broker-b");
    assert_eq!(shard["replica_offsets"][0]["node_id"], "broker-b");
    assert_eq!(shard["replica_offsets"][0]["durable_offset"], 41);
    assert_eq!(shard["leader_offset"], 44);
    assert_eq!(shard["replica_counter_offsets"][0]["durable_offset"], 7);

    assert_eq!(
        serde_json::from_value::<ReplicaStatusRequest>(json).expect("parse"),
        sent,
    );
}

/// Records when each follower was reached, with one of them answering late
/// enough that it needs a wake-up after the first has already finished.
struct TimingFollower {
    late: &'static str,
    delay: Duration,
    started: tokio::time::Instant,
    reached: Mutex<Vec<(String, Duration)>>,
}

impl PeerRequester for TimingFollower {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let InternalMessage::ReplicateRecords(batch) = message else {
            panic!("the driver sent something other than a replication batch");
        };
        if node_id == self.late {
            tokio::time::sleep(self.delay).await;
        }
        self.reached.lock().expect("lock").push((
            node_id.to_string(),
            tokio::time::Instant::now().duration_since(self.started),
        ));
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
            learner: false,
        }))
    }
}

/// **The replica report runs beside the followers, not in front of them.**
///
/// The report has to reach the control plane before the mark is published, so
/// it is awaited — but awaiting it inside the drain loop suspends every
/// follower still in flight. A slow control plane would then stall replication
/// to the rest of the replica set, which is the head-of-line block this whole
/// change exists to remove, just moved onto the reporting hop.
#[tokio::test(start_paused = true)]
async fn a_slow_control_plane_does_not_stall_the_remaining_followers() {
    const SLOW: Duration = Duration::from_secs(30);
    // Well under the control plane's delay, and long enough that this follower
    // needs a wake-up of its own after the first one has finished — which is
    // the only way to tell "shipped beside the report" from "shipped after it".
    const LATE: Duration = Duration::from_secs(5);
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let follower = TimingFollower {
        late: "broker-c",
        delay: LATE,
        started: tokio::time::Instant::now(),
        reached: Mutex::new(Vec::new()),
    };
    let marks = QuorumMarks::new();

    // A control plane that takes half a minute to answer.
    let app = axum::Router::new().route(
        "/v1/nodes/{node_id}/replica-status",
        axum::routing::post(|| async {
            tokio::time::sleep(SLOW).await;
            axum::http::StatusCode::NO_CONTENT
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });

    let report_shutdown = CancellationToken::new();
    let (reporter, _reporter_task) = crate::reporter::Reporter::spawn(
        ReportTo {
            client: reqwest::Client::new(),
            base_url: format!("http://{addr}"),
            node_id: LOCAL.to_string(),
            token: None,
            incarnation: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            storage_id: None,
        },
        report_shutdown.clone(),
    );

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        Some(&reporter),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    let reached = follower.reached.lock().expect("lock").clone();
    assert_eq!(
        reached.len(),
        2,
        "both followers should have been shipped to"
    );
    for (node, at) in reached {
        assert!(
            at < SLOW,
            "{node} was not reached until {at:?}, so it waited behind the \
             control plane rather than shipping beside it",
        );
    }

    server.abort();
}

/// A leader of `count` records on a registered stream at `consistency`.
async fn registered_leader(
    count: usize,
    consistency: felix_broker::ConsistencyLevel,
) -> (Arc<Broker>, TempDir) {
    let (broker, dir) = leader_with(count).await;
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
                consistency,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    (broker, dir)
}

/// One pass whose first report lets a publish land on the leader while it is
/// in flight: what happens when the record it releases is acknowledged and the
/// client sends the next. The pass has shipped by then, so the leader ends it
/// one record ahead of its follower. Returns the report the pass ended on.
async fn pass_with_a_publish_behind_the_report(
    broker: &Arc<Broker>,
    marks: &QuorumMarks,
) -> crate::reporter::ShardReport {
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    let published = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let app = axum::Router::new().route(
        "/v1/nodes/{node_id}/replica-status",
        axum::routing::post(move || {
            let log = log.clone();
            let published = Arc::clone(&published);
            async move {
                if !published.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    log.append(&[Bytes::from_static(b"next")])
                        .await
                        .expect("append");
                }
                axum::http::StatusCode::NO_CONTENT
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });
    let (reporter, _reporter_task) = crate::reporter::Reporter::spawn(
        ReportTo {
            client: reqwest::Client::new(),
            base_url: format!("http://{addr}"),
            node_id: LOCAL.to_string(),
            token: None,
            incarnation: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            storage_id: None,
        },
        CancellationToken::new(),
    );

    let pass = replicate_once(
        &AcceptingFollower::default(),
        broker,
        &router(LOCAL, &["broker-b"], 4),
        marks,
        Some(&reporter),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
    server.abort();
    pass.reports.last().expect("a report").clone()
}

/// **A `Quorum` leader that dies holding a record nobody else has yet can
/// still be replaced.** That record was never acknowledged: the mark moves
/// only after a report naming who holds it. A follower with everything up to
/// the mark holds everything a client was promised, and reporting it as
/// behind leaves the control plane nobody to promote. The shard then stays
/// down for good, because the only broker that could report again is dead.
#[tokio::test]
async fn a_quorum_follower_holding_every_acknowledged_record_can_lead() {
    let (broker, _dir) = registered_leader(3, felix_broker::ConsistencyLevel::Quorum).await;
    // It wrote every record itself. With no recorded start the whole log
    // would count as inherited, and nobody short of the tail is named.
    broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .record_generation(4, 0)
        .expect("record");
    let marks = QuorumMarks::new();

    let report = pass_with_a_publish_behind_the_report(&broker, &marks).await;

    assert_eq!(
        report.tail, 4,
        "the publish should have landed after shipping"
    );
    assert_eq!(marks.offset(&watch_key(&key()), 4), Some(3));
    assert_eq!(
        report.caught_up,
        vec!["broker-b".to_string()],
        "the follower holds every acknowledged record but was not offered for promotion",
    );
}

/// Under `Leader` a write is acknowledged before it ships, so the record the
/// follower lacks may already have been promised. Only an exact copy may lead.
#[tokio::test]
async fn a_leader_stream_follower_missing_the_newest_record_cannot_lead() {
    let (broker, _dir) = registered_leader(3, felix_broker::ConsistencyLevel::Leader).await;
    let marks = QuorumMarks::new();

    let report = pass_with_a_publish_behind_the_report(&broker, &marks).await;

    assert_eq!(
        report.tail, 4,
        "the publish should have landed after shipping"
    );
    assert!(report.caught_up.is_empty());
}

/// A `Quorum` leader of `records` at generation 4, which began at `records`:
/// every record it holds was inherited from an earlier leader.
async fn inheriting_leader(records: usize) -> (Arc<Broker>, TempDir) {
    let (broker, dir) = registered_leader(records, felix_broker::ConsistencyLevel::Quorum).await;
    broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .record_generation(4, records as u64)
        .expect("record");
    (broker, dir)
}

/// **A new leader names no follower that has not shown it holds what the
/// leader inherited.** Nothing of the new generation is counted yet, so the
/// mark bounds nothing, and an earlier leader may have acknowledged any of
/// the inherited records. A follower named before it answered could be
/// promoted without them, and on a promotion that is not fenced they are
/// gone.
#[tokio::test]
async fn a_new_leader_names_no_follower_before_it_answers() {
    let (broker, _dir) = inheriting_leader(3).await;
    let requester = UnreachableFollowers {
        handshake: Duration::from_millis(20),
    };

    let pass = replicate_once(
        &requester,
        &broker,
        &router(LOCAL, &["broker-b", "broker-c"], 4),
        &QuorumMarks::new(),
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    let report = pass.reports.last().expect("a report");
    assert!(
        report.caught_up.is_empty(),
        "followers that never answered were offered for promotion: {:?}",
        report.caught_up,
    );
}

/// The same leader names a follower once it answers holding the inherited
/// log.
#[tokio::test]
async fn a_new_leader_names_a_follower_holding_what_it_inherited() {
    let (broker, _dir) = inheriting_leader(3).await;

    let pass = replicate_once(
        &AcceptingFollower::default(),
        &broker,
        &router(LOCAL, &["broker-b"], 4),
        &QuorumMarks::new(),
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    let report = pass.reports.last().expect("a report");
    assert_eq!(report.caught_up, vec!["broker-b".to_string()]);
}

/// A fence that says this broker refused to open `key` at one generation.
struct RefusedAt(RefusedGeneration);

impl WriteFence for RefusedAt {
    fn quiesced(&self, _key: &crate::ShardKey) -> bool {
        true
    }

    fn refused_at(&self, _key: &crate::ShardKey) -> Option<RefusedGeneration> {
        Some(self.0.clone())
    }
}

fn refused(generation: u64) -> RefusedGeneration {
    RefusedGeneration {
        generation,
        promised_to: "broker-d".to_string(),
        ballot: 0,
    }
}

/// Scan once as the leader of a shard whose open at generation 4 was refused,
/// and return the first report the control plane got.
async fn refusal_reported(broker: &Arc<Broker>) -> ShardReplicaStatus {
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let status = ShardStatusBoard::new();
    let rebuilds = Rebuilds::new(RebuildPolicy::default());
    let throttle = MoveThrottle::unlimited();
    let (plane, received) = recording_control_plane().await;
    let shutdown = CancellationToken::new();
    let reporter = plane.reporter(shutdown.clone());
    let fence = RefusedAt(refused(4));
    let mut shards = Shards::new(Context {
        requester: &follower,
        broker,
        router: &router,
        fence: &fence,
        gate: &crate::promotion::NoGate,
        marks: &marks,
        status: &status,
        reporter: Some(&reporter),
        rebuilds: &rebuilds,
        throttle: &throttle,
        refresh: Duration::from_secs(300),
    });

    shards.scan(WakeKind::Tick);

    assert!(shards.passes.is_empty(), "a refused shard still shipped");
    assert!(follower.batches().is_empty());
    let report = first_report(&received, Duration::from_secs(5))
        .await
        .expect("no report of the refusal");
    shutdown.cancel();
    report
}

/// **A shard this broker refused to open is reported as refused, and not
/// shipped.** The control plane still names this broker at the generation it
/// promised away, and hears nothing else from it: without the report it waits
/// on silence.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_shard_reports_the_refusal_and_does_not_ship() {
    let (broker, _dir) = leader_with(3).await;

    let report = refusal_reported(&broker).await;

    assert_eq!(report.generation, 4);
    assert_eq!(report.refused, Some(refused(4)));
    assert_eq!(report.leader_offset, None, "a refusal measures no tail");
}

/// **A refusal names the stream's incarnation.** The control plane answers a
/// report of another incarnation than the assignment's as stale, so one sent
/// at 0 for a stream created since incarnations would never be acted on.
#[tokio::test(flavor = "multi_thread")]
async fn a_refusal_names_the_streams_incarnation() {
    let (broker, _dir) = incarnated_leader(5, 3).await;

    let report = refusal_reported(&broker).await;

    assert_eq!(report.refused, Some(refused(4)));
    assert_eq!(report.stream_incarnation, 5);
}

/// A refusal of an older generation than the route's says nothing about it:
/// the shard is led and shipped as usual, and no refusal is reported.
#[tokio::test(flavor = "multi_thread")]
async fn a_refusal_of_an_older_generation_does_not_hold_the_shard_back() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let status = ShardStatusBoard::new();
    let rebuilds = Rebuilds::new(RebuildPolicy::default());
    let throttle = MoveThrottle::unlimited();
    let fence = RefusedAt(refused(3));
    let mut shards = Shards::new(Context {
        requester: &follower,
        broker: &broker,
        router: &router,
        fence: &fence,
        gate: &crate::promotion::NoGate,
        marks: &marks,
        status: &status,
        reporter: None,
        rebuilds: &rebuilds,
        throttle: &throttle,
        refresh: Duration::from_secs(300),
    });

    shards.scan(WakeKind::Tick);

    assert_eq!(shards.passes.len(), 1, "the shard was held back");
}

/// Offers the fence's capabilities and answers every fence that it promised
/// the generation to broker-d.
struct PromisedAwayFollowers;

impl PeerRequester for PromisedAwayFollowers {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        match message {
            InternalMessage::Fence(fence) => Ok(InternalMessage::ReplicateError(
                felix_wire::internal::ReplicateError {
                    correlation_id: fence.correlation_id,
                    code: felix_wire::internal::ErrorCode::FencedEpoch,
                    expected_offset: 0,
                    detail: crate::replica::promised_detail(fence.shard.generation, "broker-d"),
                },
            )),
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

/// Promoted at generation 4 and never opened.
struct FencingAtFour;

#[async_trait::async_trait]
impl crate::promotion::PromotionGate for FencingAtFour {
    fn awaiting(&self, _key: &crate::ShardKey) -> Option<u64> {
        Some(4)
    }

    async fn open(&self, _key: &crate::ShardKey, _generation: u64) -> bool {
        false
    }
}

/// **A leader whose fence a majority answered with a promise to another node
/// reports the refusal.** It opened its own copy, since it persisted its
/// ballot before the elected node's fence reached it, so its open was not
/// refused; every fence it sends is. Without the report the control plane,
/// which recorded that node's election and still names this broker, waits
/// on silence.
#[tokio::test(flavor = "multi_thread")]
async fn a_fence_promised_away_by_a_majority_reports_the_refusal() {
    let (broker, _dir) = leader_with(3).await;
    let (control_plane, received) = recording_control_plane().await;
    let shutdown = CancellationToken::new();
    let driver = spawn(
        Arc::new(PromisedAwayFollowers),
        broker,
        router(LOCAL, &["broker-b", "broker-c"], 4),
        Arc::new(Unfenced),
        Arc::new(FencingAtFour),
        published(),
        Some(control_plane.reporter(shutdown.clone())),
        Duration::from_millis(50),
        Arc::default(),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        shutdown.clone(),
    );

    let report = first_report(&received, Duration::from_secs(5)).await;
    shutdown.cancel();
    driver.stop().await;
    let report = report.expect("no report of the refused fence");
    assert_eq!(report.generation, 4);
    assert_eq!(report.refused, Some(refused(4)));
    assert_eq!(report.leader_offset, None, "a refusal measures no tail");
}

/// Offers the fence's capabilities and answers every fence that its ballot
/// already holds generation 7, as a follower whose own standing persisted
/// its ballot and never opened does.
struct SupersededFollowers;

impl PeerRequester for SupersededFollowers {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        match message {
            InternalMessage::Fence(fence) => Ok(InternalMessage::ReplicateError(
                felix_wire::internal::ReplicateError {
                    correlation_id: fence.correlation_id,
                    code: felix_wire::internal::ErrorCode::FencedEpoch,
                    expected_offset: 0,
                    detail: crate::replica::superseded_detail(7, fence.shard.generation),
                },
            )),
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

/// **A leader whose fence a replica answers with a newer ballot no
/// assignment named reports the generation refused** (#1914). Elections
/// that withdrew their standings leave such ballots; the control plane
/// never hears of them, so without the report it names this broker at a
/// generation that can never be fenced and waits on silence for good.
#[tokio::test(flavor = "multi_thread")]
async fn a_fence_met_by_a_newer_ballot_reports_the_refusal() {
    let (broker, _dir) = leader_with(3).await;
    let (control_plane, received) = recording_control_plane().await;
    let shutdown = CancellationToken::new();
    let driver = spawn(
        Arc::new(SupersededFollowers),
        broker,
        router(LOCAL, &["broker-b", "broker-c"], 4),
        Arc::new(Unfenced),
        Arc::new(FencingAtFour),
        published(),
        Some(control_plane.reporter(shutdown.clone())),
        Duration::from_millis(50),
        Arc::default(),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        shutdown.clone(),
    );

    let report = first_report(&received, Duration::from_secs(5)).await;
    shutdown.cancel();
    driver.stop().await;
    let report = report.expect("no report of the refused fence");
    assert_eq!(report.generation, 4);
    assert_eq!(
        report.refused,
        Some(RefusedGeneration::set_unknown(4)),
        "refused naming no node, so placement names it again one up"
    );
}

/// The first refusal reported within `within` by a driver still fencing `key()` at generation 4,
/// with every follower taking the fence, its own copy holding a ballot at
/// `ballot` from `ballot_leader`.
async fn fence_refusal(
    ballot: u64,
    ballot_leader: &str,
    within: Duration,
) -> Option<ShardReplicaStatus> {
    let (broker, _dir) = leader_with(3).await;
    let log = broker
        .shard_log_for(
            felix_broker::LogKind::Stream,
            TENANT,
            NAMESPACE,
            STREAM,
            0,
            None,
            None,
        )
        .await
        .expect("log");
    log.accept_generation(ballot, Some(ballot_leader))
        .await
        .expect("ballot");
    let (control_plane, received) = recording_control_plane().await;
    let shutdown = CancellationToken::new();
    let driver = spawn(
        Arc::new(super::election::FenceTaker),
        broker,
        router(LOCAL, &["broker-b", "broker-c"], 4),
        Arc::new(Unfenced),
        Arc::new(FencingAtFour),
        published(),
        Some(control_plane.reporter(shutdown.clone())),
        Duration::from_millis(50),
        Arc::default(),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        shutdown.clone(),
    );
    let report = tokio::time::timeout(within, async {
        loop {
            let found = received
                .lock()
                .expect("lock")
                .iter()
                .find(|report| report.refused.is_some())
                .cloned();
            if let Some(report) = found {
                return report;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .ok();
    shutdown.cancel();
    driver.stop().await;
    report
}

/// **A promoted leader whose own copy already promised a newer generation
/// reports its generation refused while it is still fencing** (#1940). A
/// standing here that was given up leaves that promise, and so does one
/// this broker answered elsewhere; every fence a majority takes then stays pending,
/// since the open cannot take the generation's ballot on this copy, and the
/// control plane waits on silence.
#[tokio::test(flavor = "multi_thread")]
async fn a_fence_below_its_own_ballot_reports_the_refusal() {
    let report = fence_refusal(7, LOCAL, Duration::from_secs(5))
        .await
        .expect("no report of the generation its own ballot left");
    assert_eq!(report.generation, 4);
    assert_eq!(report.refused, Some(RefusedGeneration::set_unknown(4)));
}

/// **A fence below another node's ballot reports it once it has lasted
/// [`BALLOT_ELSEWHERE_FOR`], with the ballot** (#1951). Carried, placement
/// names this broker past it in one step rather than one generation per
/// report; held back, a standing that may yet open there is not deposed.
#[tokio::test(flavor = "multi_thread")]
async fn a_fence_below_another_ballot_reports_it_after_the_wait() {
    let early = BALLOT_ELSEWHERE_FOR - Duration::from_millis(500);
    assert_eq!(fence_refusal(7, "broker-b", early).await, None);
    let report = fence_refusal(7, "broker-b", BALLOT_ELSEWHERE_FOR * 2)
        .await
        .expect("no report of the generation another node's ballot left");
    assert_eq!(report.generation, 4);
    assert_eq!(report.refused, Some(RefusedGeneration::above(4, 7)));
}

/// [`PromisedAwayFollowers`] that check sets, so a fence under
/// `joint_fence` needs the set it leaves. Any fence that went out would come
/// back promised to broker-d.
struct ElectingFollowers;

impl PeerRequester for ElectingFollowers {
    async fn request(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        PromisedAwayFollowers.request(node_id, addr, message).await
    }

    async fn capabilities(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
    ) -> std::result::Result<felix_wire::internal::PeerCapabilities, PeerError> {
        Ok(crate::promotion::REQUIRED.union(felix_wire::internal::PeerCapabilities::ELECTION))
    }

    fn offers_election(&self) -> bool {
        true
    }
}

/// The first report of a leader at generation 4 under `joint_fence` whose
/// watch saw nothing before 4, with generation 4's own set kept in its log,
/// with its own ballot, as a leader keeps that opened at 4 before
/// `joint_fence` was on, or none.
/// The first report naming a refusal, or none within `within`.
async fn joint_fence_refusal(keep_own_set: bool, within: Duration) -> Option<ShardReplicaStatus> {
    use felix_common::fleet::{FleetGate, GENERATION_START, JOINT_FENCE, MAJORITY_ACK};

    let (broker, _dir) = leader_with(3).await;
    let routes = router(LOCAL, &["broker-b", "broker-c"], 4);
    if keep_own_set {
        let route = routes.snapshot().get(&key()).cloned().expect("route");
        let log = broker
            .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("log");
        log.accept_generation(4, Some(LOCAL))
            .await
            .expect("own ballot at 4");
        log.accept_set(crate::replica_set::kept(&route, None))
            .await
            .expect("keep generation 4's set");
    }
    let (control_plane, received) = recording_control_plane().await;
    let names = [GENERATION_START, MAJORITY_ACK, JOINT_FENCE].map(|feature| feature.name());
    let fleet = FleetGate::new(names.to_vec());
    fleet.restart(names.to_vec());
    let published = Published {
        marks: Arc::new(QuorumMarks::with_fleet(Arc::new(fleet))),
        ..published()
    };
    let shutdown = CancellationToken::new();
    let driver = spawn(
        Arc::new(ElectingFollowers),
        broker,
        routes,
        Arc::new(Unfenced),
        Arc::new(FencingAtFour),
        published,
        Some(control_plane.reporter(shutdown.clone())),
        Duration::from_millis(50),
        Arc::default(),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        shutdown.clone(),
    );

    let refusal = tokio::time::timeout(within, async {
        loop {
            let found = received
                .lock()
                .expect("lock")
                .iter()
                .find(|report| report.refused.is_some())
                .cloned();
            if let Some(report) = found {
                return report;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .ok();
    shutdown.cancel();
    driver.stop().await;
    refusal
}

/// **Under `joint_fence`, a leader that cannot learn the set its generation
/// leaves reports the generation refused, naming no node** (#1872). Its log
/// keeps generation 4's own set without the one before, and its watch saw
/// nothing before 4, as after a restart. Without the report its fence stays
/// pending and the control plane, which hears nothing, never names it again.
#[tokio::test(flavor = "multi_thread")]
async fn a_fence_with_an_unknown_set_before_reports_the_generation_refused() {
    let report = joint_fence_refusal(true, Duration::from_secs(5))
        .await
        .expect("no report of the unfenceable generation");
    assert_eq!(report.generation, 4);
    assert_eq!(report.refused, Some(RefusedGeneration::set_unknown(4)));
    assert_eq!(report.leader_offset, None, "a refusal measures no tail");
}

/// **A leader with no kept set reports no refusal** (#1872). It may be a
/// failover's new leader whose watch started from a snapshot: the set at 3
/// can still be acknowledging behind a partition, and a rename to 5 would
/// leave it unfenced. Its fence waits instead.
#[tokio::test(flavor = "multi_thread")]
async fn a_fence_with_no_kept_set_reports_no_refusal() {
    let refusal = joint_fence_refusal(false, Duration::from_secs(2)).await;
    assert_eq!(
        refusal, None,
        "asked for a rename with no evidence nothing came before"
    );
}

/// The first report from a driver leading `key()` at generation 4 that
/// names a refusal, its log holding a ballot at `ballot` from `ballot_leader`,
/// or none within `within`. `standing` records this broker standing at the
/// ballot's generation.
async fn ballot_refusal(
    ballot: u64,
    ballot_leader: &str,
    standing: bool,
    within: Duration,
) -> Option<ShardReplicaStatus> {
    let (broker, _dir) = leader_with(3).await;
    let log = broker
        .shard_log_for(
            felix_broker::LogKind::Stream,
            TENANT,
            NAMESPACE,
            STREAM,
            0,
            None,
            None,
        )
        .await
        .expect("log");
    log.accept_generation(ballot, Some(ballot_leader))
        .await
        .expect("ballot");
    let published = published();
    if standing {
        let elections = Arc::new(crate::election::Elections::new(
            LOCAL,
            Arc::new(tokio::sync::Notify::new()),
        ));
        elections.record(
            key(),
            crate::election::Elected {
                leader: LOCAL.to_string(),
                generation: ballot,
                set: felix_storage::disk_log::Membership {
                    generation: 3,
                    members: Vec::new(),
                },
                leaving: None,
                incarnation: 0,
            },
        );
        published.marks.set_elections(elections);
    }
    let (control_plane, received) = recording_control_plane().await;
    let shutdown = CancellationToken::new();
    let driver = spawn(
        Arc::new(AcceptingFollower::default()),
        broker,
        router(LOCAL, &["broker-b", "broker-c"], 4),
        Arc::new(Unfenced),
        Arc::new(crate::promotion::NoGate),
        published,
        Some(control_plane.reporter(shutdown.clone())),
        Duration::from_millis(50),
        Arc::default(),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        shutdown.clone(),
    );
    let refusal = tokio::time::timeout(within, async {
        loop {
            let found = received
                .lock()
                .expect("lock")
                .iter()
                .find(|report| report.refused.is_some())
                .cloned();
            if let Some(report) = found {
                return report;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .ok();
    shutdown.cancel();
    driver.stop().await;
    refusal
}

/// **An open leader whose own ballot stays past its generation reports the
/// generation refused** (#1933). A standing it gave up leaves the ballot;
/// the leader then counts its own copy for nothing, and with a follower down
/// every write waits out its quorum timeout while the rest confirm. Placement
/// names it again one up. Not before [`BALLOT_ABOVE_FOR`]: a promotion takes
/// its ballot before its route lands, and that is no refusal.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_past_its_generation_by_its_own_ballot_reports_the_refusal() {
    let early = BALLOT_ABOVE_FOR - Duration::from_millis(500);
    assert_eq!(ballot_refusal(5, LOCAL, false, early).await, None);
    let report = ballot_refusal(5, LOCAL, false, BALLOT_ABOVE_FOR * 3)
        .await
        .expect("no report of the generation its own ballot left");
    assert_eq!(report.generation, 4);
    assert_eq!(report.refused, Some(RefusedGeneration::set_unknown(4)));
}

/// **A leader whose own standing at the ballot is under way reports
/// nothing**: the standing may yet open there.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_standing_at_its_ballot_reports_no_refusal() {
    assert_eq!(
        ballot_refusal(5, LOCAL, true, BALLOT_ABOVE_FOR * 2).await,
        None
    );
}

/// **An open leader that answered another node's higher fence reports its
/// generation refused once that node has had time to open** (#1951). It
/// stepped down when its copy took the ballot; if the candidate withdraws,
/// nobody suspects this live broker and nobody stands again, so without the
/// report the shard is never served. Not before [`BALLOT_ELSEWHERE_FOR`]: a
/// candidate that opens moves the route on first.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_past_its_generation_by_another_ballot_reports_the_refusal() {
    let early = BALLOT_ELSEWHERE_FOR - Duration::from_millis(500);
    assert_eq!(ballot_refusal(5, "broker-b", false, early).await, None);
    let report = ballot_refusal(5, "broker-b", false, BALLOT_ELSEWHERE_FOR * 2)
        .await
        .expect("no report of the generation another node's ballot left");
    assert_eq!(report.generation, 4);
    assert_eq!(report.refused, Some(RefusedGeneration::above(4, 5)));
}
