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
            .apply(batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
            .await;
        taken(&handler.fence(fence(5)).await);
    }

    // Restarted, and the routing view still says generation 4.
    let broker = broker_on(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    let answer = handler
        .apply(batch(4, 1, &["late"]), felix_broker::LogKind::Stream)
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
        .apply(batch(4, 0, &["a", "b"]), felix_broker::LogKind::Stream)
        .await;

    taken(&handler.fence(fence(5)).await);

    let next = handler
        .apply(batch(4, 2, &["c"]), felix_broker::LogKind::Stream)
        .await;
    assert_eq!(refusal(&next).code, ErrorCode::FencedEpoch);

    let gap = handler
        .apply(batch(4, 9, &["far"]), felix_broker::LogKind::Stream)
        .await;
    assert_eq!(
        refusal(&gap).code,
        ErrorCode::FencedEpoch,
        "a fenced leader was told where to resume",
    );

    let bootstrap = handler
        .bootstrap(
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
        .rebuild(ReplicateRebuild {
            correlation_id: 1,
            shard: batch(4, 0, &[]).shard,
            log: ReplicaLog::Stream,
            base_offset: 0,
        })
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
        .apply(batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    handler
        .bootstrap(
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
            batch(4, 0, &["cursor"]),
            felix_broker::LogKind::GroupCursors,
        )
        .await;
    assert!(
        matches!(before, InternalMessage::ReplicateOk(_)),
        "{before:?}"
    );

    taken(&handler.fence(fence(5)).await);

    let after = handler
        .apply(batch(4, 1, &["moved"]), felix_broker::LogKind::GroupCursors)
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
            committed(4, 0, &["a", "b", "c"], 2),
            felix_broker::LogKind::Stream,
        )
        .await;

    let ok = taken(&handler.fence(fence(5)).await);

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

    let ok = taken(&handler.fence(fence(5)).await);

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
        .apply(batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    taken(&handler.fence(fence(6)).await);

    assert_eq!(
        refusal(&handler.fence(fence(5)).await).code,
        ErrorCode::FencedEpoch
    );
    assert_eq!(taken(&handler.fence(fence(6)).await).log_end, 1);
}

/// A replica whose routing view already has a newer generation than the
/// fence names knows the fencer was superseded.
#[tokio::test]
async fn a_fence_behind_the_routing_view_is_refused() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6));
    handler
        .apply(batch(6, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;

    assert_eq!(
        refusal(&handler.fence(fence(5)).await).code,
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
        .fence(Fence {
            log: ReplicaLog::GroupCursors,
            ..fence(5)
        })
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Malformed);
}

fn fetch(generation: u64, from_offset: u64) -> felix_wire::internal::ReplicateFetch {
    felix_wire::internal::ReplicateFetch {
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
        .apply(batch(4, 0, &["a", "b", "c"]), felix_broker::LogKind::Stream)
        .await;

    assert_eq!(
        refusal(&handler.fetch(fetch(5, 1)).await).code,
        ErrorCode::StaleRoute,
        "a leader that has not fenced this replica read its log",
    );
    taken(&handler.fence(fence(5)).await);
    assert_eq!(
        refusal(&handler.fetch(fetch(4, 1)).await).code,
        ErrorCode::FencedEpoch
    );

    let InternalMessage::ReplicateRecords(records) = handler.fetch(fetch(5, 1)).await else {
        panic!("expected the records");
    };
    assert_eq!(records.first_offset, 1);
    assert_eq!(
        records.payloads,
        vec![Bytes::from_static(b"b"), Bytes::from_static(b"c")]
    );
    assert_eq!(records.checksum, batch_checksum(&records.payloads, &[]));
}

/// Answers the fence and the fetch over the real transport.
struct OverTheWire(ReplicaHandler);

#[async_trait::async_trait]
impl crate::peer::PeerRequestHandler for OverTheWire {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        match request {
            InternalMessage::Fence(fence) => self.0.fence(fence).await,
            InternalMessage::ReplicateFetch(fetch) => self.0.fetch(fetch).await,
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
    marked.checksum = batch_checksum(&marked.payloads, &marked.marks);
    let answer = handler.apply(marked, felix_broker::LogKind::Stream).await;
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
