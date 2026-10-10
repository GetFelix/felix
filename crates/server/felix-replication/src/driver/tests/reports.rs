//! What the leader tells the control plane about its replicas.

use super::*;

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
                incarnation: 0,
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
            incarnation: 0,
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
    assert_eq!(shard["caught_up"][0], "broker-b");
    assert_eq!(shard["replica_offsets"][0]["node_id"], "broker-b");
    assert_eq!(shard["replica_offsets"][0]["durable_offset"], 41);
    assert_eq!(shard["leader_offset"], 44);

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
            incarnation: 0,
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
            incarnation: 0,
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
