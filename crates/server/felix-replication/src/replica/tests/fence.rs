//! A replica taking a promoted leader's fence: `AnswerFence` in
//! `docs/formal/FelixShard.tla`.

use felix_wire::internal::{Fence, FenceOk, ReplicaLog, ReplicateBootstrap, ReplicateRebuild};

use super::*;

fn fence(generation: u64) -> Fence {
    Fence {
        correlation_id: 9,
        shard: batch(generation, 0, &[]).shard,
        log: ReplicaLog::Stream,
    }
}

fn taken(answer: &InternalMessage) -> FenceOk {
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

/// The fence is kept on the shard's own log; naming one of its other logs is
/// a malformed request, not a smaller fence.
#[tokio::test]
async fn a_fence_names_the_shards_own_log() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));

    let answer = handler
        .fence(
            SENDER,
            Fence {
                log: ReplicaLog::GroupCursors,
                ..fence(5)
            },
        )
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Malformed);
}

fn fetch(generation: u64, from_offset: u64) -> felix_wire::internal::ReplicateFetch {
    felix_wire::internal::ReplicateFetch {
        timed: false,
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
        batch_checksum(&records.payloads, &[], &[])
    );
}

/// Answers the fence and the fetch over the real transport.
struct OverTheWire(ReplicaHandler);

#[async_trait::async_trait]
impl crate::peer::PeerRequestHandler for OverTheWire {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        self.handle_from(None, request).await
    }

    async fn handle_from(
        &self,
        peer: Option<Arc<str>>,
        request: InternalMessage,
    ) -> InternalMessage {
        let sender = peer.as_deref();
        match request {
            InternalMessage::Fence(fence) => self.0.fence(sender, fence).await,
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
    marked.checksum = batch_checksum(&marked.payloads, &marked.marks, &[]);
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
fn cache_router(generation: u64) -> Arc<ShardRouter> {
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
fn cache_broker(dir: &std::path::Path) -> Arc<Broker> {
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

fn cache_put(key: &str, value: &str) -> String {
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
