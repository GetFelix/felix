//! A replica taking a promoted leader's fence: `AnswerFence` in
//! `docs/formal/FelixShard.tla`.

use felix_wire::internal::{Fence, FenceOk, ReplicaLog, ReplicateBootstrap, ReplicateRebuild};

use super::*;

pub(super) fn fence(generation: u64) -> Fence {
    Fence {
        correlation_id: 9,
        shard: batch(generation, 0, &[]).shard,
        log: ReplicaLog::Stream,
        election: None,
    }
}

pub(super) fn taken(answer: &InternalMessage) -> FenceOk {
    match answer {
        InternalMessage::FenceOk(ok) => *ok,
        other => panic!("expected the fence to be taken, got {other:?}"),
    }
}

fn committed(generation: u64, first_offset: u64, values: &[&str], commit: u64) -> ReplicateRecords {
    ReplicateRecords {
        commit_offset: Some(commit),
        generations: None,
        ..batch(generation, first_offset, values)
    }
}

/// **The fence survives a restart.** It is a promise to refuse every older
/// leader, and the routing view rebuilt after a restart may still name the
/// old one. A promise that lived only in memory would let the deposed leader
/// count this broker toward a majority again.
#[tokio::test]
async fn a_fence_is_on_disk_before_it_is_answered() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
        handler
            .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
            .await;
        taken(&handler.fence(SENDER, fence(5)).await);
    }

    // Restarted, and the routing view still says generation 4.
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    let answer = handler
        .apply(
            SENDER,
            batch(4, 1, &["late"]),
            felix_broker::LogKind::Stream,
        )
        .await;

    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch);
    assert_eq!(held(&broker).await, vec!["a"]);
}

/// **Once fenced, nothing from the older leader is taken**: not a batch, not
/// a bootstrap, not a rebuild, and not a batch that would otherwise have been
/// answered with a gap and an offset to resume from. The routing view here
/// still names the old generation, as it does on a follower the control
/// plane has not reached.
#[tokio::test]
async fn an_older_leader_is_refused_after_the_fence() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    handler
        .apply(
            SENDER,
            batch(4, 0, &["a", "b"]),
            felix_broker::LogKind::Stream,
        )
        .await;

    taken(&handler.fence(SENDER, fence(5)).await);

    let next = handler
        .apply(SENDER, batch(4, 2, &["c"]), felix_broker::LogKind::Stream)
        .await;
    assert_eq!(refusal(&next).code, ErrorCode::FencedEpoch);

    let gap = handler
        .apply(SENDER, batch(4, 9, &["far"]), felix_broker::LogKind::Stream)
        .await;
    assert_eq!(
        refusal(&gap).code,
        ErrorCode::FencedEpoch,
        "a fenced leader was told where to resume",
    );

    let bootstrap = handler
        .bootstrap(
            SENDER,
            ReplicateBootstrap {
                correlation_id: 1,
                shard: batch(4, 0, &[]).shard,
                base_offset: 0,
            },
            felix_broker::LogKind::Stream,
        )
        .await;
    assert_eq!(refusal(&bootstrap).code, ErrorCode::FencedEpoch);

    let rebuild = handler
        .rebuild(
            SENDER,
            ReplicateRebuild {
                correlation_id: 1,
                shard: batch(4, 0, &[]).shard,
                log: ReplicaLog::Stream,
                base_offset: 0,
            },
        )
        .await;
    assert_eq!(refusal(&rebuild).code, ErrorCode::FencedEpoch);

    assert_eq!(held(&broker).await, vec!["a", "b"]);
}

/// **The shard's other logs are fenced with it.** The fence is kept on the
/// stream's own log, and the consumer-group cursors travel on theirs; a
/// deposed leader must not be able to move a group's position either.
#[tokio::test]
async fn the_shards_other_logs_are_fenced_with_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let dead_letters = Arc::new(
        felix_broker::DeadLetters::open(dir.path().join("dead-letters"), config.clone())
            .expect("dead letters"),
    );
    let broker = Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_durable_storage(
                felix_broker::DurableStorage::open(dir.path().join("streams"), config.clone())
                    .expect("storage"),
            )
            .with_consumer_groups(
                Arc::new(
                    felix_broker::ConsumerGroups::open(dir.path().join("groups"), config)
                        .expect("groups"),
                ),
                dead_letters,
                std::time::Duration::from_secs(30),
                3,
            ),
    );
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    handler
        .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    handler
        .bootstrap(
            SENDER,
            ReplicateBootstrap {
                correlation_id: 1,
                shard: batch(4, 0, &[]).shard,
                base_offset: 0,
            },
            felix_broker::LogKind::GroupCursors,
        )
        .await;
    let before = handler
        .apply(
            SENDER,
            batch(4, 0, &["cursor"]),
            felix_broker::LogKind::GroupCursors,
        )
        .await;
    assert!(
        matches!(before, InternalMessage::ReplicateOk(_)),
        "{before:?}"
    );

    taken(&handler.fence(SENDER, fence(5)).await);

    let after = handler
        .apply(
            SENDER,
            batch(4, 1, &["moved"]),
            felix_broker::LogKind::GroupCursors,
        )
        .await;
    assert_eq!(refusal(&after).code, ErrorCode::FencedEpoch);
}

/// **The answer says where this copy stands**: how far its log reaches, how
/// much of it is known committed, and the generation of its last record,
/// which is what the new leader orders the answers by.
#[tokio::test]
async fn the_answer_carries_the_log_end_and_the_commit_offset() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    handler
        .apply(
            SENDER,
            committed(4, 0, &["a", "b", "c"], 2),
            felix_broker::LogKind::Stream,
        )
        .await;

    let ok = taken(&handler.fence(SENDER, fence(5)).await);

    assert_eq!(ok.correlation_id, 9);
    assert_eq!(ok.log_end, 3);
    assert_eq!(ok.commit_offset, 2);
    assert_eq!(ok.last_generation, 4);
}

#[tokio::test]
async fn an_empty_replica_answers_with_an_empty_log() {
    let (broker, _dir) = broker_with_storage().await;
    broker
        .durable_storage()
        .expect("storage")
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open");
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));

    let ok = taken(&handler.fence(SENDER, fence(5)).await);

    assert_eq!(
        (ok.log_end, ok.commit_offset, ok.last_generation),
        (0, 0, 0)
    );
}

/// **A leader's promotion fences take turns; a read round's does not wait.**
/// While one of the leader's fences holds its turn, a fence at a new
/// generation waits, another leader's does not, and the read round's fence at
/// the generation the log already took is answered at once (#1796).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_leaders_promotion_fences_take_turns_and_a_read_round_does_not_wait() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = Arc::new(ReplicaHandler::new(
        Arc::clone(&broker),
        router_with(&[LOCAL], 4),
    ));
    taken(&handler.fence(SENDER, fence(5)).await);
    let soon = std::time::Duration::from_millis(500);
    // Under the turn's 2 s cap, so an answer this fast did not wait for it.
    let answered = std::time::Duration::from_millis(1_500);

    let turn = handler.fence_turn(SENDER).await.expect("the turn is free");
    let read_round = tokio::time::timeout(answered, handler.fence(SENDER, fence(5))).await;
    taken(&read_round.expect("a read round's fence does not wait for the turn"));
    let other = tokio::time::timeout(answered, handler.fence(Some("broker-b"), fence(6))).await;
    assert!(
        other.is_ok(),
        "another leader's fence does not wait for this one's turn"
    );

    let promotion = tokio::spawn({
        let handler = Arc::clone(&handler);
        async move { handler.fence(SENDER, fence(7)).await }
    });
    tokio::time::sleep(soon).await;
    assert!(
        !promotion.is_finished(),
        "a promotion fence waits for its leader's turn"
    );
    drop(turn);
    taken(&promotion.await.expect("the fence task"));
}

/// A fence whose leader's turn stays taken runs anyway once the wait is up,
/// so one stuck fence cannot hold up the leader's others past its timeout.
#[tokio::test]
async fn a_fence_runs_without_its_turn_once_the_wait_is_up() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 4));
    let _turn = handler.fence_turn(SENDER).await.expect("the turn is free");
    let waited = tokio::time::timeout(
        std::time::Duration::from_secs(4),
        handler.fence_turn(SENDER),
    )
    .await;
    assert!(
        waited.expect("the wait for a turn is bounded").is_none(),
        "the turn was never released, so the fence runs without it"
    );
}

/// A fence from a leader older than one this replica has accepted is itself
/// refused: that leader has been superseded too. Asked again at the same
/// generation, as after a lost answer, it is answered again.
#[tokio::test]
async fn a_fence_older_than_the_accepted_generation_is_refused() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    handler
        .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    taken(&handler.fence(SENDER, fence(6)).await);

    assert_eq!(
        refusal(&handler.fence(SENDER, fence(5)).await).code,
        ErrorCode::FencedEpoch
    );
    assert_eq!(taken(&handler.fence(SENDER, fence(6)).await).log_end, 1);
}

/// A replica whose routing view already has a newer generation than the
/// fence names knows the fencer was superseded.
#[tokio::test]
async fn a_fence_behind_the_routing_view_is_refused() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6));
    handler
        .apply(SENDER, batch(6, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;

    assert_eq!(
        refusal(&handler.fence(SENDER, fence(5)).await).code,
        ErrorCode::FencedEpoch
    );
}

/// **A stream shard's dead-letter log answers the fence and the fetch**, with
/// its own end, and only for the leader that fenced it. The new leader takes
/// the furthest-ahead copy of each group log by these answers.
#[tokio::test]
async fn a_group_log_answers_the_fence_and_the_fetch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let broker = Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_durable_storage(
                felix_broker::DurableStorage::open(dir.path().join("streams"), config.clone())
                    .expect("storage"),
            )
            .with_consumer_groups(
                Arc::new(
                    felix_broker::ConsumerGroups::open(dir.path().join("groups"), config.clone())
                        .expect("groups"),
                ),
                Arc::new(
                    felix_broker::DeadLetters::open(dir.path().join("dead-letters"), config)
                        .expect("dead letters"),
                ),
                std::time::Duration::from_secs(30),
                3,
            ),
    );
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    handler
        .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    let stored = handler
        .apply(
            SENDER,
            batch(4, 0, &["dl1", "dl2"]),
            felix_broker::LogKind::GroupDeadLetters,
        )
        .await;
    assert!(
        matches!(stored, InternalMessage::ReplicateOk(_)),
        "{stored:?}"
    );
    let dead_letters = |generation| Fence {
        log: ReplicaLog::GroupDeadLetters,
        ..fence(generation)
    };

    taken(&handler.fence(SENDER, fence(5)).await);
    let ok = taken(&handler.fence(SENDER, dead_letters(5)).await);
    assert_eq!((ok.log_end, ok.last_generation), (2, 4));

    let InternalMessage::ReplicateDeadLetterRecords(records) = handler
        .fetch(
            SENDER,
            felix_wire::internal::ReplicateFetch {
                log: ReplicaLog::GroupDeadLetters,
                ..fetch(5, 1)
            },
        )
        .await
    else {
        panic!("expected the dead-letter log's records");
    };
    assert_eq!(records.payloads, vec![Bytes::from_static(b"dl2")]);
    assert_eq!(
        refusal(
            &handler
                .fetch(
                    SENDER,
                    felix_wire::internal::ReplicateFetch {
                        log: ReplicaLog::GroupDeadLetters,
                        ..fetch(4, 1)
                    }
                )
                .await
        )
        .code,
        ErrorCode::FencedEpoch
    );
    // A group fence older than the stream's is refused: the stream's log is
    // where the shard's fence is kept.
    assert_eq!(
        refusal(&handler.fence(SENDER, dead_letters(4)).await).code,
        ErrorCode::FencedEpoch
    );
}

fn fetch(generation: u64, from_offset: u64) -> felix_wire::internal::ReplicateFetch {
    felix_wire::internal::ReplicateFetch {
        timed: false,
        headers: false,
        correlation_id: 4,
        shard: batch(generation, 0, &[]).shard,
        log: ReplicaLog::Stream,
        from_offset,
        max_bytes: 1 << 20,
        labelled: false,
    }
}

/// **Only the leader that fenced this replica may read its log**, and it
/// gets the records as they were shipped, from where it asked.
#[tokio::test]
async fn only_the_leader_that_fenced_the_replica_reads_its_tail() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    handler
        .apply(
            SENDER,
            batch(4, 0, &["a", "b", "c"]),
            felix_broker::LogKind::Stream,
        )
        .await;

    assert_eq!(
        refusal(&handler.fetch(SENDER, fetch(5, 1)).await).code,
        ErrorCode::StaleRoute,
        "a leader that has not fenced this replica read its log",
    );
    taken(&handler.fence(SENDER, fence(5)).await);
    assert_eq!(
        refusal(&handler.fetch(SENDER, fetch(4, 1)).await).code,
        ErrorCode::FencedEpoch
    );

    let InternalMessage::ReplicateRecords(records) = handler.fetch(SENDER, fetch(5, 1)).await
    else {
        panic!("expected the records");
    };
    assert_eq!(records.first_offset, 1);
    assert_eq!(
        records.payloads,
        vec![Bytes::from_static(b"b"), Bytes::from_static(b"c")]
    );
    assert_eq!(
        records.checksum,
        batch_checksum(&records.payloads, &[], &[], &[])
    );
}

/// Answers the fence and the fetch over the real transport.
struct OverTheWire(ReplicaHandler);

#[async_trait::async_trait]
impl crate::peer::PeerRequestHandler for OverTheWire {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        self.handle_from(None, PeerCapabilities::NONE, request)
            .await
    }

    async fn handle_from(
        &self,
        peer: Option<Arc<str>>,
        caller: PeerCapabilities,
        request: InternalMessage,
    ) -> InternalMessage {
        let sender = peer.as_deref();
        match request {
            InternalMessage::Fence(fence) => self.0.fence_from(sender, caller, fence).await,
            InternalMessage::ReplicateFetch(fetch) => self.0.fetch(sender, fetch).await,
            other => panic!("unexpected {:?}", other.kind()),
        }
    }
}

/// A tail of hundreds of records comes back across the transport, as the
/// promoted leader reads it.
#[tokio::test]
async fn a_long_tail_is_fetched_over_the_transport() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    let values: Vec<String> = (0..600).map(|i| format!("record-{i}")).collect();
    let refs: Vec<&str> = values.iter().map(String::as_str).collect();
    // Half of them from an idempotent producer, as a real stream's are.
    let mut marked = batch(4, 0, &refs);
    marked.marks = (0..600u64)
        .map(|i| {
            if i % 2 == 0 {
                felix_wire::internal::ProducerMark::Opens {
                    producer_id: 7,
                    sequence: i,
                    len: 1,
                }
            } else {
                felix_wire::internal::ProducerMark::None
            }
        })
        .collect();
    marked.checksum = batch_checksum(&marked.payloads, &marked.marks, &[], &[]);
    let answer = handler
        .apply(SENDER, marked, felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );

    let config = crate::peer::PeerTransportConfig {
        bind: "127.0.0.1:0".parse().expect("addr"),
        ..Default::default()
    };
    let server =
        crate::peer::PeerServer::bind(LOCAL.to_string(), &config, Arc::new(OverTheWire(handler)))
            .expect("bind");
    let addr = server.local_addr().expect("addr");
    let shutdown = tokio_util::sync::CancellationToken::new();
    tokio::spawn(server.serve(shutdown.clone()));
    let pool =
        crate::peer::PeerPool::new("broker-a".to_string(), config, shutdown.clone()).expect("pool");

    let answer = pool
        .request(LOCAL, addr, InternalMessage::Fence(fence(5)))
        .await
        .expect("fence");
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    let answer = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        pool.request(LOCAL, addr, InternalMessage::ReplicateFetch(fetch(5, 11))),
    )
    .await
    .expect("the fetch hung")
    .expect("fetch");
    let InternalMessage::ReplicateMarkedRecords(records) = answer else {
        panic!("expected the records, got {:?}", answer.kind());
    };
    assert_eq!(records.first_offset, 11);
    assert_eq!(records.payloads.len(), 589);
    shutdown.cancel();
}

/// A router in which `LOCAL` follows the cache shard of the same name at
/// `generation`.
pub(super) fn cache_router(generation: u64) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes: HashMap<String, NodeRef> = [
        ("broker-a".to_string(), node("broker-a", 7001)),
        (LOCAL.to_string(), node(LOCAL, 7002)),
    ]
    .into_iter()
    .collect();
    let table = RoutingTable::build(
        [(
            felix_router::ShardKey {
                kind: felix_router::ShardKind::Cache,
                ..key()
            },
            "broker-a".to_string(),
            vec![LOCAL.to_string()],
            generation,
        )],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

/// A broker with a log-backed cache and counters.
pub(super) fn cache_broker(dir: &std::path::Path) -> Arc<Broker> {
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    Arc::new(
        Broker::new(Box::new(
            felix_storage::LogCache::open(dir.join("caches"), config.clone()).expect("cache"),
        ))
        .with_counters(Arc::new(
            felix_storage::CounterStore::open(dir.join("counters"), config).expect("counters"),
        )),
    )
}

pub(super) fn cache_put(key: &str, value: &str) -> String {
    let op = felix_storage::cache::CacheOp::Put {
        key: key.to_string(),
        value: Bytes::copy_from_slice(value.as_bytes()),
        expires_at_millis: 0,
        version: None,
    };
    String::from_utf8(op.encode().to_vec()).expect("an ascii put encodes as utf8")
}

/// **A cache shard's counter log answers the fence and the fetch**, with
/// its own end and last generation, and only for the leader that fenced it.
/// The new cache leader orders the counter logs by these answers, as it
/// orders the cache logs.
#[tokio::test]
async fn a_cache_shards_counter_log_answers_the_fence_and_the_fetch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = cache_broker(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&broker), cache_router(4));
    let stored = handler
        .apply(
            SENDER,
            batch(4, 0, &["d1", "d2"]),
            felix_broker::LogKind::Counters,
        )
        .await;
    assert!(
        matches!(stored, InternalMessage::ReplicateOk(_)),
        "{stored:?}"
    );
    let counters = |generation| Fence {
        log: ReplicaLog::Counters,
        ..fence(generation)
    };

    taken(
        &handler
            .fence(
                SENDER,
                Fence {
                    log: ReplicaLog::Cache,
                    ..fence(5)
                },
            )
            .await,
    );
    let ok = taken(&handler.fence(SENDER, counters(5)).await);
    assert_eq!((ok.log_end, ok.last_generation), (2, 4));

    let InternalMessage::ReplicateCounterRecords(records) = handler
        .fetch(
            SENDER,
            felix_wire::internal::ReplicateFetch {
                log: ReplicaLog::Counters,
                ..fetch(5, 1)
            },
        )
        .await
    else {
        panic!("expected the counter log's records");
    };
    assert_eq!(records.payloads, vec![Bytes::from_static(b"d2")]);
    assert_eq!(
        refusal(
            &handler
                .fetch(
                    SENDER,
                    felix_wire::internal::ReplicateFetch {
                        log: ReplicaLog::Counters,
                        ..fetch(4, 1)
                    }
                )
                .await
        )
        .code,
        ErrorCode::FencedEpoch
    );
    let late = handler
        .apply(
            SENDER,
            batch(4, 2, &["late"]),
            felix_broker::LogKind::Counters,
        )
        .await;
    assert_eq!(refusal(&late).code, ErrorCode::FencedEpoch);
}

/// **A counter fence from a leader the cache log has seen superseded is
/// refused.** The cache log is where the shard's fence is kept.
#[tokio::test]
async fn a_counter_fence_older_than_the_cache_fence_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = cache_broker(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&broker), cache_router(4));
    taken(
        &handler
            .fence(
                SENDER,
                Fence {
                    log: ReplicaLog::Cache,
                    ..fence(6)
                },
            )
            .await,
    );

    let answer = handler
        .fence(
            SENDER,
            Fence {
                log: ReplicaLog::Counters,
                ..fence(5)
            },
        )
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch);
}

/// **A cache follower that drops a divergent suffix reads its cache as the
/// log now is.** Its index still pointed `k` at the dropped put, an offset
/// the new leader's record now holds; served after a promotion, `k` would
/// read the other key's value.
#[tokio::test]
async fn a_dropped_cache_suffix_leaves_no_trace_in_the_index() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = cache_broker(dir.path());
    let old = ReplicaHandler::new(Arc::clone(&broker), cache_router(4));
    let first = cache_put("k", "v1");
    let second = cache_put("k", "v2");
    let stored = old
        .apply(
            SENDER,
            batch(4, 0, &[&first, &second]),
            felix_broker::LogKind::Cache,
        )
        .await;
    assert!(
        matches!(stored, InternalMessage::ReplicateOk(_)),
        "{stored:?}"
    );
    let read = |key: &'static str| {
        let broker = Arc::clone(&broker);
        async move {
            broker
                .cache()
                .get(TENANT, NAMESPACE, STREAM, 0, key)
                .await
                .expect("get")
        }
    };
    assert_eq!(read("k").await.as_deref(), Some(&b"v2"[..]));

    let other = cache_put("other", "x");
    let new = ReplicaHandler::new(Arc::clone(&broker), cache_router(5));
    let stored = new
        .apply(
            SENDER,
            batch(5, 0, &[&first, &other]),
            felix_broker::LogKind::Cache,
        )
        .await;
    assert!(
        matches!(stored, InternalMessage::ReplicateOk(_)),
        "{stored:?}"
    );

    assert_eq!(read("k").await.as_deref(), Some(&b"v1"[..]));
    assert_eq!(read("other").await.as_deref(), Some(&b"x"[..]));
}

/// **One leader per generation.** Two nodes claiming the same generation is
/// what a ballot exists to stop: the second is refused a fence, a batch and a
/// read of the log, so it cannot count this replica toward a majority.
/// `Ballots` in `docs/formal/FelixShard.tla`.
#[tokio::test]
async fn a_fence_from_a_second_leader_at_the_same_generation_is_refused() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5));
    let other = Some("broker-c");
    taken(&handler.fence(SENDER, fence(5)).await);

    assert_eq!(
        refusal(&handler.fence(other, fence(5)).await).code,
        ErrorCode::FencedEpoch
    );
    let answer = handler
        .apply(
            other,
            batch(5, 0, &["theirs"]),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch);
    assert_eq!(
        refusal(&handler.fetch(other, fetch(5, 0)).await).code,
        ErrorCode::FencedEpoch
    );
    // The shard's other logs keep the ballot through the shard's own.
    let answer = handler
        .apply(
            other,
            batch(5, 0, &["cursor"]),
            felix_broker::LogKind::GroupCursors,
        )
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch);

    // The leader it promised is still answered.
    taken(&handler.fence(SENDER, fence(5)).await);
    let answer = handler
        .apply(
            SENDER,
            batch(5, 0, &["ours"]),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(held(&broker).await, vec!["ours"]);
}

/// **The ballot survives a restart.** It is on disk before the fence is
/// answered, so a replica that crashes straight after still refuses the
/// other node, though nothing in memory remembers whom it answered.
#[tokio::test]
async fn a_ballot_is_on_disk_before_the_fence_is_answered() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
        taken(&handler.fence(SENDER, fence(5)).await);
    }

    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    assert_eq!(
        refusal(&handler.fence(Some("broker-c"), fence(5)).await).code,
        ErrorCode::FencedEpoch
    );
    taken(&handler.fence(SENDER, fence(5)).await);
}

/// The ballot names the node the transport says sent the fence: two brokers
/// dialling with different node ids at one generation, and only the first
/// is answered.
#[tokio::test]
async fn the_ballot_names_the_peer_that_said_hello() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    let config = crate::peer::PeerTransportConfig {
        bind: "127.0.0.1:0".parse().expect("addr"),
        ..Default::default()
    };
    let server =
        crate::peer::PeerServer::bind(LOCAL.to_string(), &config, Arc::new(OverTheWire(handler)))
            .expect("bind");
    let addr = server.local_addr().expect("addr");
    let shutdown = tokio_util::sync::CancellationToken::new();
    tokio::spawn(server.serve(shutdown.clone()));

    let first =
        crate::peer::PeerPool::new("broker-a".to_string(), config.clone(), shutdown.clone())
            .expect("pool");
    let second =
        crate::peer::PeerPool::new("broker-c".to_string(), config, shutdown.clone()).expect("pool");
    let answer = first
        .request(LOCAL, addr, InternalMessage::Fence(fence(5)))
        .await
        .expect("fence");
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    let answer = second
        .request(LOCAL, addr, InternalMessage::Fence(fence(5)))
        .await
        .expect("fence");
    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch);
    shutdown.cancel();
}

/// `LOCAL` as it led the shard at generation 5 before a crash: a record, and
/// its own ballot.
pub(super) async fn led_before_a_crash(dir: &std::path::Path) {
    let broker = broker_on(dir);
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    handler
        .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.accept_generation(5, Some(LOCAL)).await.expect("lead");
}

fn unclean() -> Arc<crate::unclean::UncleanStart> {
    Arc::new(crate::unclean::UncleanStart::new(LOCAL))
}

/// **A leader that came back from a crash answers no promotion fence from
/// the copy it led (#1295)**, not after a clean restart either, until a newer
/// leader ships to it. Its tail may be short of what it acknowledged, and
/// counted in its successor's majority it could stand in for a follower that
/// holds the lost records.
#[tokio::test]
async fn a_crashed_leaders_copy_takes_no_fence_until_a_newer_leader_ships() {
    let dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(dir.path()).await;
    {
        let broker = broker_on(dir.path());
        let handler =
            ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL, "broker-c"], 6))
                .with_unclean_start(unclean());
        assert_eq!(
            refusal(&handler.fence(SENDER, fence(6)).await).code,
            ErrorCode::Unavailable
        );
    }

    // Restarted cleanly: the refusal was kept on disk.
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL, "broker-c"], 6));
    assert_eq!(
        refusal(&handler.fence(SENDER, fence(6)).await).code,
        ErrorCode::Unavailable
    );

    // The new leader opened on a majority without it, and ships.
    let answer = handler
        .apply(
            SENDER,
            batch(6, 0, &["a", "b"]),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    taken(&handler.fence(SENDER, fence(7)).await);
}

/// [`led_before_a_crash`], with the generation-start record it wrote at 5
/// still on disk: whatever the crash cost it was written at 5.
async fn led_with_its_start_record(dir: &std::path::Path) {
    led_before_a_crash(dir).await;
    let broker = broker_on(dir);
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    let tail = log.tail_offset().await.expect("tail");
    log.record_generation(5, tail).expect("record generation");
    log.append_generation_start(5).await.expect("start record");
}

/// broker-a promoted at `generation` over `members`, by a failover whose
/// assignment named `acknowledged` as the set before it.
fn failover_router(members: &[&str], generation: u64, acknowledged: &[&str]) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes: HashMap<String, NodeRef> = ["broker-a", LOCAL, "broker-c", "broker-d"]
        .into_iter()
        .zip(7001..)
        .map(|(id, port)| (id.to_string(), node(id, port)))
        .collect();
    let table = RoutingTable::build_with(
        [felix_router::Placed {
            key: key(),
            leader: "broker-a".to_string(),
            replicas: members.iter().map(|r| r.to_string()).collect(),
            generation,
            draining: false,
            successor: None,
            routing: felix_wire::routing::ShardRouting::Modulo,
            incarnation: 0,
            fence_storage_ids: acknowledged
                .iter()
                .map(|id| (id.to_string(), format!("storage-{id}")))
                .collect(),
            member_storage_ids: Default::default(),
            pending: None,
        }],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

/// In a set of two the other copy holds every record the two acknowledged,
/// so the crashed leader still takes its fence: refusing would leave the
/// shard with no majority at all.
#[tokio::test]
async fn a_crashed_leader_of_two_copies_takes_the_fence() {
    let dir = tempfile::tempdir().expect("tempdir");
    led_with_its_start_record(dir.path()).await;
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(
        Arc::clone(&broker),
        failover_router(&[LOCAL], 6, &["broker-a", LOCAL]),
    )
    .with_unclean_start(unclean());
    taken(&handler.fence(SENDER, fence(6)).await);
}

/// **A later fence is not the set of two's (#1638).** This broker led
/// generation 5 and its view still shows 6 as `{broker-a, broker-d}`, but the
/// fence is for 8, from a leader of a set of three that view predates:
/// refused. A fence from another sender at 6 is refused too.
#[tokio::test]
async fn a_crashed_leader_refuses_a_fence_past_the_set_of_two() {
    for (sender, generation) in [(SENDER, 8), (Some(LOCAL), 6)] {
        let dir = tempfile::tempdir().expect("tempdir");
        led_with_its_start_record(dir.path()).await;
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(
            Arc::clone(&broker),
            failover_router(&[LOCAL], 6, &["broker-a", LOCAL]),
        )
        .with_unclean_start(unclean());
        assert_eq!(
            refusal(&handler.fence(sender, fence(generation)).await).code,
            ErrorCode::Unavailable
        );
    }
}

/// **A set that shrank to two after acknowledging is refused (#1336).** The
/// generation this broker led had three copies, so a record it lost may be
/// on broker-c alone, which the set of two the routes show now leaves out.
/// The hand-built router stands in for the shape of a staged successor the
/// failover left out (a current control plane never sends a shrunk set's
/// dropped copy), so the refusal names broker-c and is counted.
#[tokio::test]
async fn a_crashed_leader_of_a_set_that_shrank_to_two_refuses_the_fence() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = ::metrics::set_default_local_recorder(&recorder);
    let dir = tempfile::tempdir().expect("tempdir");
    led_with_its_start_record(dir.path()).await;
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(
        Arc::clone(&broker),
        failover_router(&[LOCAL], 6, &["broker-a", LOCAL, "broker-c"]),
    )
    .with_unclean_start(unclean());
    let answer = handler.fence(SENDER, fence(6)).await;
    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::Unavailable);
    assert!(refused.detail.contains("broker-c"), "{}", refused.detail);
    assert!(
        refused.detail.contains("staged or joining successor"),
        "{}",
        refused.detail
    );
    assert!(
        handle.render().contains(&format!(
            "{} 1",
            crate::peer::metrics::FENCE_REFUSED_IDS_BEYOND_TWO_TOTAL
        )),
        "{}",
        handle.render()
    );
}

/// **A staged successor in the fence ids is named in the refusal (#1635).**
/// The set of two is right but the ids also name broker-c, so the copy
/// refuses, as it must: the leader may have counted broker-c. No retry ends
/// that, so the refusal names broker-c and the possible causes, and it is counted.
/// A refusal for another reason says neither.
#[tokio::test]
async fn a_crashed_leader_beside_a_staged_successor_names_it_and_the_way_out() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = ::metrics::set_default_local_recorder(&recorder);
    let dir = tempfile::tempdir().expect("tempdir");
    led_with_its_start_record(dir.path()).await;
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(
        Arc::clone(&broker),
        failover_router(&[LOCAL], 6, &["broker-a", LOCAL, "broker-c"]),
    )
    .with_unclean_start(unclean());

    let answer = handler.fence(SENDER, fence(6)).await;

    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::Unavailable);
    assert!(refused.detail.contains("broker-c"), "{}", refused.detail);
    assert!(
        refused.detail.contains("staged or joining successor"),
        "{}",
        refused.detail
    );
    assert!(
        handle.render().contains(&format!(
            "{} 1",
            crate::peer::metrics::FENCE_REFUSED_IDS_BEYOND_TWO_TOTAL
        )),
        "{}",
        handle.render()
    );

    // Without the start record nothing says the losses are the two's: the
    // plain refusal, not counted.
    let dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(dir.path()).await;
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(
        Arc::clone(&broker),
        failover_router(&[LOCAL], 6, &["broker-a", LOCAL, "broker-c"]),
    )
    .with_unclean_start(unclean());
    let answer = handler.fence(SENDER, fence(6)).await;
    assert!(!refusal(&answer).detail.contains("abandon"), "{answer:?}");
    assert!(
        handle.render().contains(&format!(
            "{} 1",
            crate::peer::metrics::FENCE_REFUSED_IDS_BEYOND_TWO_TOTAL
        )),
        "{}",
        handle.render()
    );
}

/// Without the start record of the generation it led, the crash may have
/// cost this broker records of an earlier generation, whose set nothing
/// here names: refused even with two copies now.
#[tokio::test]
async fn a_crashed_leader_that_lost_its_start_record_refuses_the_fence() {
    let dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(dir.path()).await;
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(
        Arc::clone(&broker),
        failover_router(&[LOCAL], 6, &["broker-a", LOCAL]),
    )
    .with_unclean_start(unclean());
    assert_eq!(
        refusal(&handler.fence(SENDER, fence(6)).await).code,
        ErrorCode::Unavailable
    );
}

/// A set of two that is not the failover from the generation this broker
/// led says nothing of the set that led to it: placement wrote another
/// assignment in between, or this broker never took the last one. Refused.
#[tokio::test]
async fn a_crashed_leader_of_two_copies_refuses_a_later_assignment() {
    for (router, generation) in [
        (failover_router(&[LOCAL], 7, &["broker-a", LOCAL]), 7),
        (router_with(&[LOCAL], 6), 6),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        led_with_its_start_record(dir.path()).await;
        let broker = broker_on(dir.path());
        let handler =
            ReplicaHandler::new(Arc::clone(&broker), router).with_unclean_start(unclean());
        assert_eq!(
            refusal(&handler.fence(SENDER, fence(generation)).await).code,
            ErrorCode::Unavailable
        );
    }
}

/// A copy this broker only followed before the crash is answered for as
/// before.
#[tokio::test]
async fn a_followed_copy_takes_the_fence_after_an_unclean_start() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5));
        handler
            .apply(SENDER, batch(5, 0, &["a"]), felix_broker::LogKind::Stream)
            .await;
    }
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL, "broker-c"], 6))
        .with_unclean_start(unclean());
    taken(&handler.fence(SENDER, fence(6)).await);
}

/// In a set of four, during a replacement or not, the set that acknowledged
/// may have been three, where this broker's vote could stand in for a record
/// it lost: refused, as in any set but two.
#[tokio::test]
async fn a_crashed_leader_of_four_copies_refuses_the_fence() {
    let dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(dir.path()).await;
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(
        Arc::clone(&broker),
        router_with(&[LOCAL, "broker-c", "broker-d"], 6),
    )
    .with_unclean_start(unclean());
    assert_eq!(
        refusal(&handler.fence(SENDER, fence(6)).await).code,
        ErrorCode::Unavailable
    );
}

/// A batch a leader ships after an unclean start, labelled with records of
/// its own generation, vouches for the copy: a leader has compared it since
/// the crash.
#[tokio::test]
async fn a_batch_vouches_for_the_copy_after_an_unclean_start() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5));
        handler
            .apply(SENDER, batch(5, 0, &["a"]), felix_broker::LogKind::Stream)
            .await;
    }
    let broker = broker_on(dir.path());
    let start = unclean();
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5))
        .with_unclean_start(Arc::clone(&start));
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    let shard = crate::driver::watch_key(&key());
    assert!(
        start.unvouched(&shard, &log, 6),
        "followed at 5, named at 6"
    );

    let answer = handler
        .apply(
            SENDER,
            labelled(5, 5, 0, &["a", "b"]),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert!(!start.unvouched(&shard, &log, 6));
}

fn no_routes() -> Arc<ShardRouter> {
    Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ))
}

/// **A copy with no promise answers no fence its routing view cannot back
/// (#1267).** A wiped broker's ballots went with the directory, so a leader
/// its old copy had refused would otherwise count it. Nothing is persisted
/// for the refusal, and once the view has the shard the same leader is
/// answered.
#[tokio::test]
async fn a_copy_with_no_promise_takes_no_fence_its_routes_do_not_have() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), no_routes());

    let answer = handler.fence(SENDER, fence(5)).await;
    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::StaleRoute);
    assert!(refused.code.is_retryable());
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    assert_eq!(log.accepted_generation(), 0);
    assert!(log.accepted_leader().is_none());

    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5));
    taken(&handler.fence(SENDER, fence(5)).await);
}

/// **The wipe, end to end over the peer transport.** The replica took
/// broker-a's fence at 5, its directory is wiped, and it starts again, twice, the
/// second time with the storage id the first one wrote. Its view does not
/// have the shard yet. The deposed leader's fence at 4 is refused both
/// times: nothing on disk says which start was the wiped one.
#[tokio::test]
async fn a_wiped_copy_refuses_a_deposed_leaders_fence_across_restarts() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5));
        taken(&handler.fence(SENDER, fence(5)).await);
    }
    std::fs::remove_dir_all(dir.path()).expect("wipe");
    std::fs::create_dir_all(dir.path()).expect("recreate");

    for start in ["after the wipe", "after a second restart"] {
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(Arc::clone(&broker), no_routes());
        let config = crate::peer::PeerTransportConfig {
            bind: "127.0.0.1:0".parse().expect("addr"),
            ..Default::default()
        };
        let server = crate::peer::PeerServer::bind(
            LOCAL.to_string(),
            &config,
            Arc::new(OverTheWire(handler)),
        )
        .expect("bind");
        let addr = server.local_addr().expect("addr");
        let shutdown = tokio_util::sync::CancellationToken::new();
        tokio::spawn(server.serve(shutdown.clone()));
        let deposed = crate::peer::PeerPool::new("broker-c".to_string(), config, shutdown.clone())
            .expect("pool");
        let answer = deposed
            .request(LOCAL, addr, InternalMessage::Fence(fence(4)))
            .await
            .expect("fence");
        assert_eq!(refusal(&answer).code, ErrorCode::StaleRoute, "{start}");
        shutdown.cancel();
    }
}

/// At the generation its routing view has, a copy with no promise answers
/// only the leader the view names: the control plane named one leader there,
/// and any ballot a wiped directory gave at it was that leader's. A newer
/// generation was never promised, and is answered as before.
#[tokio::test]
async fn a_copy_with_no_promise_answers_only_the_routed_leader_at_the_routed_generation() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5));

    assert_eq!(
        refusal(&handler.fence(Some("broker-c"), fence(5)).await).code,
        ErrorCode::FencedEpoch
    );
    taken(&handler.fence(Some("broker-c"), fence(6)).await);
}

/// Once a copy has taken a generation, its ballot answers: the routing view
/// is no longer needed.
#[tokio::test]
async fn a_copy_with_a_promise_answers_by_its_ballot() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    let answer = handler
        .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );

    let handler = ReplicaHandler::new(Arc::clone(&broker), no_routes());
    taken(&handler.fence(SENDER, fence(5)).await);
    assert_eq!(
        refusal(&handler.fence(SENDER, fence(4)).await).code,
        ErrorCode::FencedEpoch
    );
}

/// A control plane that answers a confirmed read with `answer`, counting
/// how often it was asked.
struct ConfirmedAs {
    answer: crate::confirmed::Confirmed,
    asked: std::sync::atomic::AtomicUsize,
}

impl ConfirmedAs {
    fn new(answer: crate::confirmed::Confirmed) -> Arc<Self> {
        Arc::new(Self {
            answer,
            asked: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn asked(&self) -> usize {
        self.asked.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl crate::confirmed::ConfirmedRoutes for ConfirmedAs {
    async fn confirmed(&self, _key: &felix_router::ShardKey) -> crate::confirmed::Confirmed {
        self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.answer.clone()
    }
}

/// **A lagging routing view does not answer for a wiped copy (#1332).** The
/// view still names broker-a at 5, but the control plane has already put
/// broker-c at 6. With no promise on disk, the copy refuses broker-a's fence
/// and its batch, and writes nothing for either.
#[tokio::test]
async fn a_copy_with_no_promise_refuses_a_leader_its_lagging_view_still_names() {
    let (broker, _dir) = broker_with_storage().await;
    let confirmed = ConfirmedAs::new(crate::confirmed::Confirmed::Assigned {
        generation: 6,
        leader: "broker-c".to_string(),
    });
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5))
        .with_confirmed_routes(Arc::clone(&confirmed) as _);

    assert_eq!(
        refusal(&settled(|| handler.fence(SENDER, fence(5))).await).code,
        ErrorCode::FencedEpoch
    );
    // Answered from the read the fence made.
    let answer = handler
        .apply(SENDER, batch(5, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch);
    assert_eq!(confirmed.asked(), 1);
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    assert_eq!(log.accepted_generation(), 0);
    assert!(log.accepted_leader().is_none());
    assert_eq!(log.tail_offset().await.expect("tail"), 0);
}

/// The confirmed read only adds refusals. An unanswered read backs nothing
/// and is retried; a control plane that does not serve it leaves the view to
/// answer as before; and once the copy holds a promise it is not asked.
#[tokio::test]
async fn a_confirmed_read_only_adds_refusals_to_a_copy_with_no_promise() {
    use crate::confirmed::Confirmed;
    let (broker, _dir) = broker_with_storage().await;
    let unavailable = ConfirmedAs::new(Confirmed::Unavailable);
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5))
        .with_confirmed_routes(Arc::clone(&unavailable) as _);
    for _ in 0..3 {
        let refused = refusal(&handler.fence(SENDER, fence(5)).await).clone();
        assert_eq!(refused.code, ErrorCode::StaleRoute);
        assert!(refused.code.is_retryable());
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    assert!(
        unavailable.asked() >= 2,
        "an unanswered read is tried again"
    );
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    assert_eq!(log.accepted_generation(), 0);

    let not_served = ConfirmedAs::new(Confirmed::NotServed);
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5))
        .with_confirmed_routes(Arc::clone(&not_served) as _);
    assert_eq!(
        refusal(&handler.fence(Some("broker-c"), fence(5)).await).code,
        ErrorCode::FencedEpoch,
        "the view still refuses what it refused before"
    );

    let current = ConfirmedAs::new(Confirmed::Assigned {
        generation: 5,
        leader: "broker-a".to_string(),
    });
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5))
        .with_confirmed_routes(Arc::clone(&current) as _);
    taken(&settled(|| handler.fence(SENDER, fence(5))).await);
    let asked = current.asked();
    assert_eq!(asked, 1);
    let answer = handler
        .apply(SENDER, batch(5, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    taken(&handler.fence(SENDER, fence(5)).await);
    assert_eq!(current.asked(), asked, "a copy with a promise asks nothing");
}

/// The same lagging view, for a bootstrap and a rebuild: either would set
/// the ballot of a copy with no promise from the deposed leader.
#[tokio::test]
async fn a_copy_with_no_promise_refuses_a_lagging_views_leader_a_bootstrap() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5))
        .with_confirmed_routes(ConfirmedAs::new(crate::confirmed::Confirmed::Assigned {
            generation: 6,
            leader: "broker-c".to_string(),
        }) as _);
    let answer = settled(|| {
        handler.bootstrap(
            SENDER,
            ReplicateBootstrap {
                correlation_id: 1,
                shard: batch(5, 0, &[]).shard,
                base_offset: 0,
            },
            felix_broker::LogKind::Stream,
        )
    })
    .await;
    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch);
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    assert_eq!(log.accepted_generation(), 0);
    assert!(log.accepted_leader().is_none());
}

#[tokio::test]
async fn a_copy_with_no_promise_refuses_a_lagging_views_leader_a_rebuild() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5))
        .with_confirmed_routes(ConfirmedAs::new(crate::confirmed::Confirmed::Assigned {
            generation: 6,
            leader: "broker-c".to_string(),
        }) as _);
    let answer = settled(|| {
        handler.rebuild(
            SENDER,
            ReplicateRebuild {
                correlation_id: 1,
                shard: batch(5, 0, &[]).shard,
                log: ReplicaLog::Stream,
                base_offset: 0,
            },
        )
    })
    .await;
    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch);
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    assert_eq!(log.accepted_generation(), 0);
    assert!(log.accepted_leader().is_none());
}

/// A refusal from a broker that predates [`RefusalReason`]: its detail alone.
fn old_peer(detail: String) -> ReplicateError {
    ReplicateError {
        correlation_id: 0,
        code: ErrorCode::FencedEpoch,
        expected_offset: 0,
        detail,
        reason: None,
    }
}

/// **A leader reads a refusal's reason from its fields when it has them**,
/// not from the detail, whose wording is for people.
#[test]
fn a_refusal_with_a_reason_is_read_from_its_fields() {
    let with = |reason: RefusalReason| ReplicateError {
        detail: "reworded".to_string(),
        reason: Some(reason),
        ..old_peer(String::new())
    };
    let promised = with(RefusalReason::PromisedTo {
        generation: 4,
        leader: "broker-d".to_string(),
    });
    assert_eq!(promised_to(&promised, 4), Some("broker-d"));
    assert_eq!(promised_to(&promised, 5), None, "another generation");
    assert!(crate::replica::refuses_leader(&promised, 4));
    assert_eq!(superseded_by(&promised, 4), None);

    let accepted = with(RefusalReason::Accepted {
        accepted: 6,
        sender: 4,
    });
    assert_eq!(superseded_by(&accepted, 4), Some(6));
    assert_eq!(superseded_by(&accepted, 5), None, "another sender");
    assert_eq!(promised_to(&accepted, 4), None);
    assert!(!crate::replica::refuses_leader(&accepted, 4));

    let view = with(RefusalReason::View { have: 6, sender: 4 });
    assert_eq!(superseded_by(&view, 4), Some(6));

    let assigned = with(RefusalReason::Assigned {
        assigned: 6,
        sender: 4,
    });
    assert!(crate::replica::assigned_newer(&assigned, 4));
    assert!(
        !crate::replica::assigned_newer(&assigned, 5),
        "another sender"
    );
    assert!(!crate::replica::assigned_newer(&accepted, 4));

    let not_a_replica = ReplicateError {
        code: ErrorCode::Unauthorized,
        ..with(RefusalReason::NotAReplica)
    };
    assert!(crate::replica::refuses_leader(&not_a_replica, 4));
}

/// **A leader reads whom a refused generation was promised to.** It tells a
/// ballot cast for another node at its own generation from a newer leader,
/// and only the first says the control plane recorded an election it did
/// not adopt.
#[test]
fn a_promise_refusal_names_the_node_it_was_promised_to() {
    let promised = promised_detail(4, "broker-d");

    assert_eq!(
        promised_to(&old_peer(promised.clone()), 4),
        Some("broker-d")
    );
    assert_eq!(
        promised_to(&old_peer(promised.clone()), 5),
        None,
        "another generation"
    );
    assert_eq!(
        promised_to(
            &old_peer("this broker accepted generation 6, the sender is at 4".to_string()),
            4
        ),
        None,
        "a newer generation is not a promise"
    );
}

/// **A leader reads the newer ballot a replica refused it for**, and only
/// that: a promise at its own generation, or a newer generation refused
/// against another sender's, is not one.
#[test]
fn a_superseded_refusal_names_the_newer_ballot() {
    assert_eq!(
        superseded_by(&old_peer(superseded_detail(6, 4)), 4),
        Some(6)
    );
    assert_eq!(
        superseded_by(&old_peer(superseded_detail(6, 4)), 5),
        None,
        "another sender"
    );
    assert_eq!(
        superseded_by(&old_peer(promised_detail(4, "broker-d")), 4),
        None
    );
    assert_eq!(
        superseded_by(&old_peer(superseded_detail(4, 4)), 4),
        None,
        "not newer"
    );
}

/// **A routing view above the sender's is read as a newer generation too.**
/// A replica that took a candidate's fence answers from its view before its
/// ballot is checked, and that refusal must not read as an unknown one.
#[test]
fn a_newer_routing_view_names_the_newer_generation() {
    assert_eq!(superseded_by(&old_peer(view_detail(6, 4)), 4), Some(6));
    assert_eq!(
        superseded_by(&old_peer(view_detail(6, 4)), 5),
        None,
        "another sender"
    );
    assert_eq!(
        superseded_by(&old_peer(view_detail(4, 4)), 4),
        None,
        "not newer"
    );
}

/// **Only a newer assignment reads as one.** A ballot or view names no leader.
#[test]
fn a_newer_assignment_is_read_from_its_refusal() {
    use crate::replica::{assigned_detail, assigned_newer};
    assert!(assigned_newer(&old_peer(assigned_detail(6, 4)), 4));
    assert!(
        !assigned_newer(&old_peer(assigned_detail(6, 4)), 5),
        "another sender"
    );
    assert!(
        !assigned_newer(&old_peer(assigned_detail(4, 4)), 4),
        "not newer"
    );
    assert!(!assigned_newer(&old_peer(superseded_detail(6, 4)), 4));
    assert!(!assigned_newer(&old_peer(view_detail(6, 4)), 4));
}

/// A control plane that names broker-a at generation 1 for every shard, but
/// answers for any shard other than 0 only once `open` has permits.
struct GatedConfirmed {
    open: tokio::sync::Semaphore,
    asked: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl crate::confirmed::ConfirmedRoutes for GatedConfirmed {
    async fn confirmed(&self, key: &felix_router::ShardKey) -> crate::confirmed::Confirmed {
        self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if key.shard != 0 {
            drop(self.open.acquire().await);
        }
        crate::confirmed::Confirmed::Assigned {
            generation: 1,
            leader: "broker-a".to_string(),
        }
    }
}

/// A router in which `LOCAL` follows shards `0..shards` of the stream, each
/// led by broker-a at generation 1.
fn router_for_shards(shards: u32) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes: HashMap<String, NodeRef> = [
        ("broker-a".to_string(), node("broker-a", 7001)),
        (LOCAL.to_string(), node(LOCAL, 7002)),
    ]
    .into_iter()
    .collect();
    let routes = (0..shards).map(|shard| {
        (
            felix_router::ShardKey { shard, ..key() },
            "broker-a".to_string(),
            vec![LOCAL.to_string()],
            1,
        )
    });
    router.publish(RoutingTable::build(routes, &nodes), &nodes);
    router
}

fn fence_of(shard: u32) -> Fence {
    let mut fence = fence(1);
    fence.shard.shard = shard;
    fence
}

/// **A slow control plane does not hold up the lane (#1791).** The peer
/// server answers one request at a time per lane. New shards' copies hold no
/// promise, so each first fence needs a confirmed read; made on the lane,
/// every read would stand in front of the batch queued behind them for a
/// shard this copy already backs. Here the reads do not answer until the
/// batch is in.
#[tokio::test]
async fn confirmed_reads_for_new_shards_do_not_hold_up_the_lane() {
    const NEW_SHARDS: u32 = 8;
    let (broker, _dir) = broker_with_storage().await;
    let control_plane = Arc::new(GatedConfirmed {
        open: tokio::sync::Semaphore::new(0),
        asked: std::sync::atomic::AtomicUsize::new(0),
    });
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_for_shards(NEW_SHARDS + 1))
        .with_confirmed_routes(Arc::clone(&control_plane) as _);
    // Shard 0 is backed: its copy took the fence.
    taken(&settled(|| handler.fence(SENDER, fence_of(0))).await);

    // In lane order: the new shards' fences, then shard 0's batch.
    let lane = async {
        for shard in 1..=NEW_SHARDS {
            let answer = handler.fence(SENDER, fence_of(shard)).await;
            assert!(reading(&answer), "{answer:?}");
        }
        handler
            .apply(SENDER, batch(1, 0, &["a"]), felix_broker::LogKind::Stream)
            .await
    };
    let answer = tokio::time::timeout(Duration::from_secs(10), lane)
        .await
        .expect("the lane waited on the control plane");
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );

    // Each new shard's retry is answered from its own read.
    control_plane
        .open
        .add_permits(tokio::sync::Semaphore::MAX_PERMITS);
    let opened = std::time::Instant::now();
    for shard in 1..=NEW_SHARDS {
        taken(&settled(|| handler.fence(SENDER, fence_of(shard))).await);
    }
    // A read per shard, not per request, plus the refreshes a slow run
    // lets through: a 500 ms answer is read again at least every 250 ms.
    let asked = control_plane
        .asked
        .load(std::sync::atomic::Ordering::SeqCst);
    let shards = NEW_SHARDS as usize + 1;
    let refreshes = 1 + opened.elapsed().as_millis() as usize / 250;
    assert!(
        (shards..=shards * (1 + refreshes)).contains(&asked),
        "{asked} reads"
    );
}

/// A router in which broker-a leads the shard's `incarnation` at `generation`
/// with `LOCAL` as its follower, the set naming `LOCAL` under `local_id`.
fn router_naming(generation: u64, incarnation: u64, local_id: [u8; 16]) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes: HashMap<String, NodeRef> = ["broker-a", LOCAL]
        .into_iter()
        .zip(7001..)
        .map(|(id, port)| (id.to_string(), node(id, port)))
        .collect();
    let table = RoutingTable::build_with(
        [felix_router::Placed {
            key: key(),
            leader: "broker-a".to_string(),
            replicas: vec![LOCAL.to_string()],
            generation,
            draining: false,
            successor: None,
            routing: Default::default(),
            incarnation,
            fence_storage_ids: Default::default(),
            member_storage_ids: [
                (
                    "broker-a".to_string(),
                    felix_storage::StorageId::from_bytes([1; 16]).to_hex(),
                ),
                (
                    LOCAL.to_string(),
                    felix_storage::StorageId::from_bytes(local_id).to_hex(),
                ),
            ]
            .into(),
            pending: None,
        }],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

/// **A new shard's first fence is taken without a confirmed read (#1791).**
/// Its creation set names this copy's current storage id, so no earlier copy
/// here held the shard and there is no lost promise to check for. The
/// control plane here never answers; the fence is taken anyway.
#[tokio::test]
async fn a_new_shards_first_fence_needs_no_confirmed_read() {
    const OWN: [u8; 16] = [2; 16];
    let (broker, _dir) = broker_with_storage().await;
    super::incarnation::register(&broker, 7).await;
    let control_plane = Arc::new(GatedConfirmed {
        open: tokio::sync::Semaphore::new(0),
        asked: std::sync::atomic::AtomicUsize::new(0),
    });
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_naming(1, 7, OWN))
        .with_local_storage(felix_storage::StorageId::from_bytes(OWN))
        .with_confirmed_routes(Arc::clone(&control_plane) as _);
    let mut first = fence(1);
    first.shard.incarnation = Some(7);
    // Without the creation set, a first fence is refused while the read runs.
    taken(&handler.fence(SENDER, first).await);
    assert_eq!(
        control_plane
            .asked
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

/// Only the creation set of an incarnation is evidence. One naming this copy
/// at a later generation, naming another storage id, or of a shard with no
/// incarnation (deleted and re-created at generation 1, storage id kept)
/// still needs the read.
#[tokio::test]
async fn a_later_set_or_another_storage_id_still_needs_the_read() {
    const OWN: [u8; 16] = [2; 16];
    for (generation, incarnation, named) in [(5, 7, OWN), (1, 7, [3; 16]), (1, 0, OWN)] {
        let (broker, _dir) = broker_with_storage().await;
        if incarnation != 0 {
            super::incarnation::register(&broker, incarnation).await;
        }
        let control_plane = Arc::new(GatedConfirmed {
            open: tokio::sync::Semaphore::new(0),
            asked: std::sync::atomic::AtomicUsize::new(0),
        });
        let handler = ReplicaHandler::new(
            Arc::clone(&broker),
            router_naming(generation, incarnation, named),
        )
        .with_local_storage(felix_storage::StorageId::from_bytes(OWN))
        .with_confirmed_routes(Arc::clone(&control_plane) as _);
        let mut request = fence(generation);
        request.shard.incarnation = (incarnation != 0).then_some(incarnation);
        let answer = handler.fence(SENDER, request).await;
        assert!(
            reading(&answer),
            "generation {generation} incarnation {incarnation}: {answer:?}"
        );
    }
}

/// Requests that arrive while a shard's read is under way wait on it rather
/// than start their own, and the answer is reused for the shard's next ones.
#[tokio::test]
async fn one_confirmed_read_answers_every_request_for_a_shard() {
    let (broker, _dir) = broker_with_storage().await;
    let current = ConfirmedAs::new(crate::confirmed::Confirmed::Assigned {
        generation: 5,
        leader: "broker-a".to_string(),
    });
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5))
        .with_confirmed_routes(Arc::clone(&current) as _);
    let first = handler.fence(SENDER, fence(5)).await;
    let second = handler.fence(SENDER, fence(5)).await;
    assert!(reading(&first) && reading(&second), "{first:?} {second:?}");
    taken(&settled(|| handler.fence(SENDER, fence(5))).await);
    assert_eq!(current.asked(), 1);
}

/// What the confirmed check costs per request, for the pull request's
/// numbers: `cargo test -p felix-replication --release --lib
/// confirmed_check_cost -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "a timing report, not a check"]
async fn confirmed_check_cost() {
    const CHECKS: u32 = 1_000_000;
    const BATCHES: u64 = 2_000;
    let (broker, _dir) = broker_with_storage().await;
    let current = ConfirmedAs::new(crate::confirmed::Confirmed::Assigned {
        generation: 5,
        leader: "broker-a".to_string(),
    });
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5))
        .with_confirmed_routes(Arc::clone(&current) as _);
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

    // A copy with no promise, answered from a held read.
    assert!(reading(
        &handler
            .refuse_unconfirmed(9, &key(), &log, 5, SENDER, None)
            .expect("reading")
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let started = std::time::Instant::now();
    for _ in 0..CHECKS {
        assert!(
            handler
                .refuse_unconfirmed(9, &key(), &log, 5, SENDER, None)
                .is_none()
        );
    }
    let fresh_copy = started.elapsed() / CHECKS;

    // A copy that holds a promise.
    taken(&handler.fence(SENDER, fence(5)).await);
    let started = std::time::Instant::now();
    for _ in 0..CHECKS {
        assert!(
            handler
                .refuse_unconfirmed(9, &key(), &log, 5, SENDER, None)
                .is_none()
        );
    }
    let promised_copy = started.elapsed() / CHECKS;

    let started = std::time::Instant::now();
    for batch_no in 0..BATCHES {
        let answer = handler
            .apply(
                SENDER,
                batch(5, batch_no, &["a"]),
                felix_broker::LogKind::Stream,
            )
            .await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "{answer:?}"
        );
    }
    let apply = started.elapsed() / BATCHES as u32;
    eprintln!(
        "confirmed check: fresh copy {fresh_copy:?}, promised copy {promised_copy:?}; \
         batch apply (promised, 1 record) {apply:?}"
    );
}

/// **A ballot this broker wrote standing for election after an unclean
/// start is not evidence that it led before the crash (#2025).** It only
/// followed the shard; refusing a newer candidate's fence over its own
/// candidacy would leave a set of three with a dead leader no majority.
#[tokio::test]
async fn a_copy_that_stood_after_an_unclean_start_takes_a_newer_fence() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5));
        handler
            .apply(SENDER, batch(5, 0, &["a"]), felix_broker::LogKind::Stream)
            .await;
    }
    let broker = broker_on(dir.path());
    let start = unclean();
    let handler = ReplicaHandler::new(
        Arc::clone(&broker),
        router_led_by("broker-c", &[LOCAL, "broker-a"], 7),
    )
    .with_unclean_start(Arc::clone(&start));
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    // The candidacy, as `stand` in election/candidate.rs takes it: its own
    // ballot one generation up, then no majority.
    start.stand(&crate::driver::watch_key(&key()), &log, 6);
    log.accept_generation(6, Some(LOCAL)).await.expect("stand");

    taken(&handler.fence(Some("broker-c"), fence(7)).await);
}

/// A batch from `leader` at `generation` whose records are labelled as
/// written at `written`.
pub(super) fn labelled(
    generation: u64,
    written: u64,
    first_offset: u64,
    values: &[&str],
) -> ReplicateRecords {
    ReplicateRecords {
        generations: Some(vec![felix_wire::internal::GenerationStart {
            generation: written,
            start_offset: first_offset,
        }]),
        ..batch(generation, first_offset, values)
    }
}

/// **A batch that brings back only part of what a crashed leader lost does
/// not vouch for its copy (#2029).** `LOCAL` led at 5 and its crash lost
/// records it acknowledged there. The next leader ships the first of them
/// back at 6: the copy now names that leader in its ballot but is still
/// short, so it answers no other fence and may not stand. Counted, it and an
/// empty third copy would elect a leader without the record it still lacks.
/// A batch that leaves it level with the shipping leader up to where that
/// leader's generation starts vouches for it: everything the leader held
/// comes before that offset.
#[tokio::test]
async fn a_partly_reshipped_copy_is_not_vouched_for_until_it_reaches_the_leaders_generation() {
    let dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(dir.path()).await;
    let broker = broker_on(dir.path());
    let start = unclean();
    let leader = ReplicaHandler::new(
        Arc::clone(&broker),
        router_led_by("broker-c", &[LOCAL, "broker-a"], 6),
    )
    .with_unclean_start(Arc::clone(&start));
    let next = ReplicaHandler::new(
        Arc::clone(&broker),
        router_led_by("broker-a", &[LOCAL, "broker-c"], 7),
    )
    .with_unclean_start(Arc::clone(&start));
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    let shard = crate::driver::watch_key(&key());

    // A record it led at 5 comes back from the leader at 6.
    let answer = leader
        .apply(
            Some("broker-c"),
            labelled(6, 5, 0, &["a"]),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(log.accepted_leader().as_deref(), Some("broker-c"));
    assert!(start.led_before(&shard, &log, 7), "still short");
    assert!(start.unvouched(&shard, &log, 7), "may not stand");
    assert_eq!(
        refusal(&next.fence(SENDER, fence(7)).await).code,
        ErrorCode::Unavailable
    );

    // The copy reaches where generation 6 starts: everything the leader
    // held came first.
    let answer = leader
        .apply(
            Some("broker-c"),
            labelled(6, 6, 1, &["start"]),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert!(!start.led_before(&shard, &log, 7));
    assert!(!start.unvouched(&shard, &log, 7));
    taken(&next.fence(SENDER, fence(7)).await);
}

/// **A batch that names the leader's generation starting past what the copy
/// holds does not vouch for it.** The copy is level with the batch but not
/// yet with where the leader's generation starts, and the records between
/// may be ones the crash lost.
#[tokio::test]
async fn a_batch_short_of_where_the_leaders_generation_starts_does_not_vouch() {
    let dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(dir.path()).await;
    let broker = broker_on(dir.path());
    let start = unclean();
    let leader = ReplicaHandler::new(
        Arc::clone(&broker),
        router_led_by("broker-c", &[LOCAL, "broker-a"], 6),
    )
    .with_unclean_start(Arc::clone(&start));
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");

    let answer = leader
        .apply(
            Some("broker-c"),
            ReplicateRecords {
                generations: Some(vec![
                    felix_wire::internal::GenerationStart {
                        generation: 4,
                        start_offset: 0,
                    },
                    felix_wire::internal::GenerationStart {
                        generation: 6,
                        start_offset: 2,
                    },
                ]),
                ..batch(6, 0, &["a"])
            },
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(log.tail_offset().await.expect("tail"), 1, "level at 1");
    assert!(
        start.unvouched(&crate::driver::watch_key(&key()), &log, 7),
        "generation 6 starts at 2"
    );
}

/// **An unlabelled batch does not vouch for a crashed leader's copy.** It
/// labels what it carried with the sender's generation, the record the
/// crashed leader wrote at 5 included, so the copy would look as if it held
/// the new leader's generation while still short of what it acknowledged.
#[tokio::test]
async fn an_unlabelled_batch_does_not_vouch_for_a_copy_led_before_the_crash() {
    let dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(dir.path()).await;
    let broker = broker_on(dir.path());
    let start = unclean();
    let leader = ReplicaHandler::new(
        Arc::clone(&broker),
        router_led_by("broker-c", &[LOCAL, "broker-a"], 6),
    )
    .with_unclean_start(Arc::clone(&start));
    let next = ReplicaHandler::new(
        Arc::clone(&broker),
        router_led_by("broker-a", &[LOCAL, "broker-c"], 7),
    )
    .with_unclean_start(Arc::clone(&start));
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    let shard = crate::driver::watch_key(&key());

    // Two records the crashed leader wrote at 5 come back, unlabelled.
    for (offset, value) in [(0, "a"), (1, "b")] {
        let answer = leader
            .apply(
                Some("broker-c"),
                batch(6, offset, &[value]),
                felix_broker::LogKind::Stream,
            )
            .await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "{answer:?}"
        );
    }
    assert!(
        log.holds_records_of(6),
        "labelled with the sender's generation"
    );
    assert!(start.unvouched(&shard, &log, 7), "may not stand");
    assert_eq!(
        refusal(&next.fence(SENDER, fence(7)).await).code,
        ErrorCode::Unavailable
    );
}

/// Records nothing about a peer until something dials it, as the pool does.
struct FirstContact {
    handler: ReplicaHandler,
    dialled: std::sync::atomic::AtomicBool,
}

impl FirstContact {
    const OFFERED: felix_wire::internal::PeerCapabilities =
        felix_wire::internal::PeerCapabilities::GENERATION_LABELS
            .union(felix_wire::internal::PeerCapabilities::RECORD_TIMES)
            .union(felix_wire::internal::PeerCapabilities::RECORD_HEADERS);
}

impl crate::peer::PeerRequester for FirstContact {
    async fn request(
        &self,
        _node_id: &str,
        _addr: std::net::SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, crate::peer::PeerError> {
        self.dialled
            .store(true, std::sync::atomic::Ordering::SeqCst);
        match message {
            InternalMessage::ReplicateRecords(batch)
            | InternalMessage::ReplicateMarkedRecords(batch) => Ok(self
                .handler
                .apply(Some("broker-c"), batch, felix_broker::LogKind::Stream)
                .await),
            other => panic!("unexpected {:?}", other.kind()),
        }
    }

    async fn capabilities(
        &self,
        _node_id: &str,
        _addr: std::net::SocketAddr,
    ) -> std::result::Result<felix_wire::internal::PeerCapabilities, crate::peer::PeerError> {
        self.dialled
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(Self::OFFERED)
    }

    fn recorded_capabilities(
        &self,
        _node_id: &str,
    ) -> Option<felix_wire::internal::PeerCapabilities> {
        self.dialled
            .load(std::sync::atomic::Ordering::SeqCst)
            .then_some(Self::OFFERED)
    }
}

/// **A new leader's first batch to a crashed leader's copy is labelled**, so
/// it vouches for the copy even when the shard goes idle after it. Unlabelled,
/// it could not, and with no further records to ship the copy would stay
/// doubted and refuse every fence.
#[tokio::test]
async fn a_first_contact_batch_vouches_for_the_copy_on_an_idle_shard() {
    let (tail, vouched) = ship_to_a_crashed_leaders_copy(true).await;
    assert_eq!(tail, 2, "shipped");
    assert!(vouched, "vouched on first contact");
}

/// **A leader that writes no start record still vouches for the copy**, as
/// on a fleet that has not finalized `generation_start`. It opened after
/// fencing a majority, so a copy level with it up to where its generation
/// starts holds every acknowledged record, whether or not a record of that
/// generation follows.
#[tokio::test]
async fn a_leader_without_a_start_record_vouches_for_the_copy_on_an_idle_shard() {
    let (tail, vouched) = ship_to_a_crashed_leaders_copy(false).await;
    assert_eq!(tail, 1, "nothing past where generation 6 starts");
    assert!(vouched, "vouched without a record of generation 6");
}

/// A leader at 6 ships to the copy a crashed leader at 5 kept, until there
/// is nothing more to ship. Returns the copy's tail and whether it was
/// vouched for.
async fn ship_to_a_crashed_leaders_copy(start_record: bool) -> (u64, bool) {
    let follower_dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(follower_dir.path()).await;
    let follower = broker_on(follower_dir.path());
    let start = unclean();
    let requester = FirstContact {
        handler: ReplicaHandler::new(
            Arc::clone(&follower),
            router_led_by("broker-c", &[LOCAL, "broker-c"], 6),
        )
        .with_unclean_start(Arc::clone(&start)),
        dialled: std::sync::atomic::AtomicBool::new(false),
    };

    let leader_dir = tempfile::tempdir().expect("tempdir");
    let leader = broker_on(leader_dir.path());
    ReplicaHandler::new(Arc::clone(&leader), router_with(&[LOCAL], 4))
        .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    let leader_log = leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    leader_log
        .accept_generation(6, Some("broker-c"))
        .await
        .expect("lead");
    leader_log.record_generation(6, 1).expect("record");
    if start_record {
        leader_log.append_generation_start(6).await.expect("start");
    }

    let mut shard = batch(6, 0, &[]).shard;
    shard.generation = 6;
    let mut cursor =
        crate::follower::FollowerCursor::new(LOCAL, "10.0.0.4:7002".parse().expect("addr"), 0);
    for _ in 0..4 {
        crate::ship::ship_once(
            &requester,
            &leader_log,
            &shard,
            felix_broker::LogKind::Stream,
            &mut cursor,
            1 << 20,
            &crate::rebuild::Rebuilds::disabled(),
        )
        .await;
    }

    let log = follower
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    let tail = log.tail_offset().await.expect("tail");
    (
        tail,
        !start.unvouched(&crate::driver::watch_key(&key()), &log, 7),
    )
}

pub(super) fn kept_marks() -> Arc<crate::quorum::QuorumMarks> {
    let marks = Arc::new(crate::quorum::QuorumMarks::new());
    marks.set_kept_ballots(true);
    marks
}

pub(super) fn attested(
    kept_from: u64,
    trusted: bool,
) -> Option<felix_wire::internal::FenceAttestation> {
    Some(felix_wire::internal::FenceAttestation { kept_from, trusted })
}

/// **A clean start attests kept ballots and a trusted copy, and only a
/// directory that kept its ballots does.** Without the record a broker
/// counts for no set and covers nothing (`CountNonKept`, `TrustsRegistered`
/// in `docs/formal/FelixShardUnified.tla`). A group log carries no
/// attestation: nothing vouches for it apart from the shard's own log.
#[tokio::test]
async fn a_fence_answer_attests_what_the_directory_kept() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_on(dir.path());
    let kept =
        ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4)).with_fleet(kept_marks());
    kept.apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert_eq!(
        taken(&unified_fence(&kept, SENDER, fence(5)).await).attestation,
        attested(0, true)
    );

    let lost = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6))
        .with_fleet(Arc::new(crate::quorum::QuorumMarks::new()));
    assert_eq!(
        taken(&unified_fence(&lost, SENDER, fence(6)).await).attestation,
        attested(u64::MAX, false)
    );
}

/// **A poisoned log takes the trust away at once.** A failed flush may have
/// dropped acknowledged pages in this process, and the next start only marks
/// the directory unclean. The ballots are synced apart from the log, so the
/// copy still promises.
#[tokio::test]
async fn a_poisoned_log_attests_no_trusted_copy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_on(dir.path());
    let mut handler =
        ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4)).with_fleet(kept_marks());
    handler.poisoned = || true;
    handler
        .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert_eq!(
        taken(&unified_fence(&handler, SENDER, fence(5)).await).attestation,
        attested(0, false)
    );
}

/// **After an unclean start a copy is trusted only once a batch leaves it
/// level through both the shipping leader's start and the commit offset it
/// carries, above zero.** The mark alone can sit below records an earlier
/// generation acknowledged (`VouchBelowStart`), the start alone is short of
/// what the leader acknowledged since (`StandInNoCatchUp`), and a commit
/// offset of zero only picks a frame layout. The copy is vouched for as
/// before at the start; trust is the stricter of the two.
#[tokio::test]
async fn an_unclean_copy_is_trusted_only_level_through_the_start_and_the_mark() {
    let dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(dir.path()).await;
    let broker = broker_on(dir.path());
    let start = unclean();
    let leader = ReplicaHandler::new(
        Arc::clone(&broker),
        router_led_by("broker-c", &[LOCAL, "broker-a"], 6),
    )
    .with_fleet(kept_marks())
    .with_unclean_start(Arc::clone(&start));
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    let shard = crate::driver::watch_key(&key());
    let ship = |records: ReplicateRecords| {
        let leader = &leader;
        async move {
            let answer = leader
                .apply(Some("broker-c"), records, felix_broker::LogKind::Stream)
                .await;
            assert!(
                matches!(answer, InternalMessage::ReplicateOk(_)),
                "{answer:?}"
            );
        }
    };
    let from_six = |first_offset: u64, values: &[&str], commit: Option<u64>| ReplicateRecords {
        commit_offset: commit,
        generations: Some(vec![felix_wire::internal::GenerationStart {
            generation: 6,
            start_offset: 1,
        }]),
        ..batch(6, first_offset, values)
    };
    assert_eq!(
        leader.attestation(&key(), true, false),
        attested(0, false).expect("some")
    );

    // Level through the mark, short of where generation 6 starts.
    ship(ReplicateRecords {
        commit_offset: Some(1),
        ..labelled(6, 4, 0, &["a"])
    })
    .await;
    assert!(!start.trusted(&shard), "below the start record");

    // Level through the start with a mark of zero.
    ship(from_six(1, &["start"], Some(0))).await;
    assert!(
        !start.unvouched(&shard, &log, 7),
        "vouched for at the start"
    );
    assert!(!start.trusted(&shard), "a mark of zero vouches for nothing");

    // Through the start, short of the mark.
    ship(from_six(2, &["w"], Some(4))).await;
    assert!(!start.trusted(&shard), "short of the mark");

    // Through both.
    ship(from_six(3, &["x"], Some(4))).await;
    assert!(start.trusted(&shard));
    assert_eq!(
        leader.attestation(&key(), true, false),
        attested(0, true).expect("some")
    );
}

/// **An unclean copy level with an idle leader is trusted by its last record
/// sent again with the mark.** The batch that brought the start record went
/// before any follower's answer raised the mark, so it carried zero. The
/// leader sends the copy its last record again once the mark covers it, and
/// that batch, wholly a record the copy holds, vouches for it
/// (`FelixShardUnifiedIdleVouch`). Below the start record it would not: see
/// [`an_unclean_copy_is_trusted_only_level_through_the_start_and_the_mark`].
#[tokio::test]
async fn an_unclean_copy_level_with_an_idle_leader_is_trusted_by_its_last_record_again() {
    let dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(dir.path()).await;
    let broker = broker_on(dir.path());
    let start = unclean();
    let leader = ReplicaHandler::new(
        Arc::clone(&broker),
        router_led_by("broker-c", &[LOCAL, "broker-a"], 6),
    )
    .with_fleet(kept_marks())
    .with_unclean_start(Arc::clone(&start));
    let shard = crate::driver::watch_key(&key());
    let start_record = |commit: u64| ReplicateRecords {
        commit_offset: Some(commit),
        generations: Some(vec![felix_wire::internal::GenerationStart {
            generation: 6,
            start_offset: 1,
        }]),
        ..batch(6, 1, &["start"])
    };
    let answer = leader
        .apply(
            Some("broker-c"),
            labelled(6, 4, 0, &["a"]),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    for (commit, trusted) in [(0, false), (2, true)] {
        let answer = leader
            .apply(
                Some("broker-c"),
                start_record(commit),
                felix_broker::LogKind::Stream,
            )
            .await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(ref ok) if ok.durable_offset == 2),
            "{answer:?}"
        );
        assert_eq!(start.trusted(&shard), trusted, "a mark of {commit}");
    }
    assert_eq!(
        leader.attestation(&key(), true, false),
        attested(0, true).expect("some")
    );
}

/// A fence from a caller that offered `UNIFIED_FENCE`.
async fn unified_fence(
    handler: &ReplicaHandler,
    sender: Option<&str>,
    fence: Fence,
) -> InternalMessage {
    handler
        .fence_from(sender, PeerCapabilities::UNIFIED_FENCE, fence)
        .await
}

/// **To a caller that counts by attestation, a crashed leader's copy answers
/// untrusted (#1295).** Its ballots survived, so its promise counts; its log
/// may be short, so it covers nothing. Without the bit it refuses as before.
/// The doubt goes with the ballot the copy takes, so a later fence from an
/// older leader is still refused, and every ballot refusal stands.
#[tokio::test]
async fn a_crashed_leaders_copy_answers_a_unified_fence_untrusted() {
    let dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(dir.path()).await;
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL, "broker-c"], 6))
        .with_fleet(kept_marks())
        .with_unclean_start(unclean());
    assert_eq!(
        refusal(&handler.fence(SENDER, fence(6)).await).code,
        ErrorCode::Unavailable
    );

    let ok = taken(&unified_fence(&handler, SENDER, fence(6)).await);
    assert_eq!(ok.attestation, attested(0, false));
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    assert_eq!(log.accepted_generation(), 6);

    assert_eq!(
        refusal(&handler.fence(SENDER, fence(7)).await).code,
        ErrorCode::Unavailable,
        "the copy took a ballot and no batch: it is still the one led before"
    );
    let ok = taken(&unified_fence(&handler, SENDER, fence(7)).await);
    assert_eq!(ok.attestation, attested(0, false));
    assert_eq!(
        refusal(&unified_fence(&handler, SENDER, fence(6)).await).code,
        ErrorCode::FencedEpoch
    );
    assert_eq!(
        refusal(&unified_fence(&handler, Some("broker-c"), fence(7)).await).code,
        ErrorCode::FencedEpoch
    );
}

/// **A copy given up before a clean restart refuses a unified fence too.**
/// Nothing in this process tracks its doubt, and taking a ballot would
/// overwrite the one record of it.
#[tokio::test]
async fn a_given_up_copy_after_a_clean_restart_refuses_a_unified_fence() {
    let dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(dir.path()).await;
    {
        let broker = broker_on(dir.path());
        let handler =
            ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL, "broker-c"], 6))
                .with_unclean_start(unclean());
        assert_eq!(
            refusal(&handler.fence(SENDER, fence(6)).await).code,
            ErrorCode::Unavailable
        );
    }
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL, "broker-c"], 6))
        .with_fleet(kept_marks());
    assert_eq!(
        refusal(&unified_fence(&handler, SENDER, fence(6)).await).code,
        ErrorCode::Unavailable
    );
}

/// **The set-of-two exception does not decide a unified fence (#1336).** A
/// crashed leader of a set that shrank to two refuses an older caller, since
/// the record it lost may be on the dropped copy alone. A caller that counts
/// by attestation gets an untrusted answer instead.
#[tokio::test]
async fn a_crashed_leader_of_a_shrunk_set_answers_a_unified_fence_untrusted() {
    let dir = tempfile::tempdir().expect("tempdir");
    led_with_its_start_record(dir.path()).await;
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(
        Arc::clone(&broker),
        failover_router(&[LOCAL], 6, &["broker-a", LOCAL, "broker-c"]),
    )
    .with_fleet(kept_marks())
    .with_unclean_start(unclean());
    assert_eq!(
        refusal(&handler.fence(SENDER, fence(6)).await).code,
        ErrorCode::Unavailable
    );
    let ok = taken(&unified_fence(&handler, SENDER, fence(6)).await);
    assert_eq!(ok.attestation, attested(0, false));
}

/// **A copy given up on disk is never trusted.** After a clean restart the
/// set of two still lets this crashed leader's copy answer, but its log may
/// be short of what it acknowledged before the crash.
#[tokio::test]
async fn a_given_up_copy_of_a_set_of_two_answers_untrusted() {
    let dir = tempfile::tempdir().expect("tempdir");
    led_with_its_start_record(dir.path()).await;
    {
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(
            Arc::clone(&broker),
            failover_router(&[LOCAL], 6, &["broker-a", LOCAL, "broker-c"]),
        )
        .with_unclean_start(unclean());
        assert_eq!(
            refusal(&handler.fence(SENDER, fence(6)).await).code,
            ErrorCode::Unavailable
        );
    }
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(
        Arc::clone(&broker),
        failover_router(&[LOCAL], 6, &["broker-a", LOCAL]),
    )
    .with_fleet(kept_marks());
    let ok = taken(&unified_fence(&handler, SENDER, fence(6)).await);
    assert_eq!(ok.attestation, attested(0, false));
}

/// **Over the wire, a crashed leader's copy refuses a caller that does not
/// offer `UNIFIED_FENCE`**, as `FELIX_UNIFIED_FENCE=0` leaves it. Its answer
/// in place of a refusal is safe only facing a leader that counts by the
/// attestation.
#[tokio::test]
async fn over_the_wire_a_crashed_leaders_copy_still_refuses() {
    let dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(dir.path()).await;
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL, "broker-c"], 6))
        .with_fleet(kept_marks())
        .with_unclean_start(unclean());
    let config = crate::peer::PeerTransportConfig {
        bind: "127.0.0.1:0".parse().expect("addr"),
        unified_fence: false,
        ..Default::default()
    };
    let server =
        crate::peer::PeerServer::bind(LOCAL.to_string(), &config, Arc::new(OverTheWire(handler)))
            .expect("bind");
    let addr = server.local_addr().expect("addr");
    let shutdown = tokio_util::sync::CancellationToken::new();
    tokio::spawn(server.serve(shutdown.clone()));
    let pool =
        crate::peer::PeerPool::new("broker-a".to_string(), config, shutdown.clone()).expect("pool");
    let answer = pool
        .request(LOCAL, addr, InternalMessage::Fence(fence(6)))
        .await
        .expect("fence");
    assert_eq!(refusal(&answer).code, ErrorCode::Unavailable, "{answer:?}");
    shutdown.cancel();
}

/// **A unified fence the ballot refuses leaves the crashed leader's copy as
/// it was.** The doubt is noted before the ballot is taken; a refused ballot
/// must not move the copy or what it attests.
#[tokio::test]
async fn a_refused_unified_fence_leaves_a_crashed_leaders_copy_unchanged() {
    let dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(dir.path()).await;
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL, "broker-c"], 6))
        .with_fleet(kept_marks())
        .with_unclean_start(unclean());
    taken(&unified_fence(&handler, SENDER, fence(6)).await);

    assert_eq!(
        refusal(&unified_fence(&handler, Some("broker-c"), fence(6)).await).code,
        ErrorCode::FencedEpoch
    );
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    assert_eq!(log.accepted_generation(), 6);
    assert_eq!(log.accepted_leader().as_deref(), SENDER);
    assert_eq!(
        refusal(&handler.fence(SENDER, fence(7)).await).code,
        ErrorCode::Unavailable,
        "still the copy led before"
    );
    let ok = taken(&unified_fence(&handler, SENDER, fence(7)).await);
    assert_eq!(ok.attestation, attested(0, false));
}
