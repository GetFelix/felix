//! Retention on a `Quorum` leader whose followers are behind (#1094).

use std::sync::atomic::{AtomicBool, Ordering};

use felix_wire::internal::{ErrorCode, ReplicateError};

use super::*;

/// Followers that can be stopped, each keeping an honest account of the
/// offsets it holds: contiguous records from a base to a tail.
#[derive(Default)]
struct StoppableFollowers {
    stopped: AtomicBool,
    held: Mutex<Map<String, (u64, u64)>>,
}

impl StoppableFollowers {
    fn holds(&self, offset: u64) -> bool {
        self.held
            .lock()
            .expect("lock")
            .values()
            .any(|(base, tail)| (*base..*tail).contains(&offset))
    }
}

fn refusal(code: ErrorCode, expected_offset: u64) -> InternalMessage {
    InternalMessage::ReplicateError(ReplicateError {
        correlation_id: 0,
        code,
        expected_offset,
        detail: String::new(),
    })
}

impl PeerRequester for StoppableFollowers {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "stopped".to_string(),
            });
        }
        let mut held = self.held.lock().expect("lock");
        let (base, tail) = held.entry(node_id.to_string()).or_default();
        let answer = match message {
            InternalMessage::ReplicateRecords(batch)
            | InternalMessage::ReplicateMarkedRecords(batch) => {
                if batch.first_offset != *tail {
                    refusal(ErrorCode::LogGap, *tail)
                } else {
                    *tail += batch.payloads.len() as u64;
                    InternalMessage::ReplicateOk(ReplicateOk {
                        correlation_id: 0,
                        durable_offset: *tail,
                    })
                }
            }
            // As `ReplicaHandler::bootstrap`: an empty copy moves to the
            // offered base, one spanning it stays, anything else is a hole.
            InternalMessage::ReplicateBootstrap(offer) => {
                if *base == *tail {
                    (*base, *tail) = (offer.base_offset, offer.base_offset);
                }
                if *base <= offer.base_offset && offer.base_offset <= *tail {
                    InternalMessage::ReplicateOk(ReplicateOk {
                        correlation_id: 0,
                        durable_offset: *tail,
                    })
                } else {
                    refusal(ErrorCode::LogConflict, *tail)
                }
            }
            InternalMessage::ReplicateRebuild(request) => {
                (*base, *tail) = (request.base_offset, request.base_offset);
                InternalMessage::ReplicateOk(ReplicateOk {
                    correlation_id: 0,
                    durable_offset: request.base_offset,
                })
            }
            other => panic!("unexpected {:?}", other.kind()),
        };
        Ok(answer)
    }
}

/// **The mark never passes a record no replica holds.**
///
/// Both followers stop while the leader keeps taking records, which a
/// `Quorum` publish waits on. A tight retention bound then deletes the
/// leader's oldest segments. Unclamped, it deletes records nobody else has;
/// the followers come back below the new base, are rebuilt there, and their
/// answers carry the mark over the gap, acknowledging the waiting publish
/// for records that are gone.
#[tokio::test]
async fn retention_and_rebuilds_never_carry_the_mark_over_a_lost_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            segment_size_bytes: 128,
            index_spacing_bytes: 64,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            retention_bytes: Some(256),
            retention_check_interval: std::time::Duration::from_secs(3600),
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
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
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let followers = StoppableFollowers::default();
    let marks = QuorumMarks::new();
    let watched = watch_key(&key());
    // The production default: one automatic rebuild at a time.
    let rebuilds = Rebuilds::new(crate::RebuildPolicy::default());
    let (mut cursors, mut group, mut dead, mut counters) = (
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    );
    let mut pass = async || {
        replicate_once_with(
            &followers,
            &broker,
            &router,
            &Unfenced,
            &crate::promotion::NoGate,
            &marks,
            None,
            &mut cursors,
            &mut group,
            &mut dead,
            &mut counters,
            &rebuilds,
            &MoveThrottle::unlimited(),
        )
        .await;
    };

    for i in 0..2 {
        log.append(&[Bytes::from(format!("value-{i:03}"))])
            .await
            .expect("append");
    }
    pass().await;
    let committed = marks.offset(&watched, 4).expect("a mark");
    assert_eq!(committed, 2);

    followers.stopped.store(true, Ordering::SeqCst);
    for i in 2..40 {
        log.append(&[Bytes::from(format!("value-{i:03}"))])
            .await
            .expect("append");
    }
    pass().await;
    assert_eq!(marks.offset(&watched, 4), Some(committed));
    log.enforce_retention_now().await.expect("retention");

    followers.stopped.store(false, Ordering::SeqCst);
    for _ in 0..10 {
        pass().await;
    }

    let mark = marks.offset(&watched, 4).unwrap_or(0);
    let base = log.base_offset();
    let lost: Vec<u64> = (committed..mark)
        .filter(|offset| *offset < base && !followers.holds(*offset))
        .collect();
    assert!(
        lost.is_empty(),
        "the mark reached {mark}, acknowledging {lost:?}, which no replica holds \
         (the leader's log now begins at {base})",
    );
    let tail = log.tail_offset().await.expect("tail");
    assert_eq!(mark, tail, "the followers never caught up");
}

/// **A shard left with no follower is not held.** It is its own majority, and
/// no pass runs for it to advance the commit offset, so a hold kept from when
/// it had followers would stop its retention for good once the log reopened
/// with it (#1109).
#[tokio::test]
async fn a_shard_whose_followers_are_removed_is_no_longer_held() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            segment_size_bytes: 128,
            index_spacing_bytes: 64,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            retention_bytes: Some(256),
            retention_check_interval: std::time::Duration::from_secs(3600),
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
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
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    let followers = StoppableFollowers::default();
    followers.stopped.store(true, Ordering::SeqCst);
    let marks = QuorumMarks::new();
    let (mut cursors, mut group, mut dead, mut counters) = (
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    );
    for i in 0..40 {
        log.append(&[Bytes::from(format!("value-{i:03}"))])
            .await
            .expect("append");
    }

    let replicated = router(LOCAL, &["broker-b", "broker-c"], 4);
    replicate_once(
        &followers,
        &broker,
        &replicated,
        &marks,
        None,
        &mut cursors,
        &mut group,
        &mut dead,
        &mut counters,
    )
    .await;
    log.enforce_retention_now().await.expect("retention");
    assert_eq!(
        log.base_offset(),
        0,
        "nothing is committed, so nothing goes"
    );

    let alone = router(LOCAL, &[], 4);
    replicate_once(
        &followers,
        &broker,
        &alone,
        &marks,
        None,
        &mut cursors,
        &mut group,
        &mut dead,
        &mut counters,
    )
    .await;
    log.enforce_retention_now().await.expect("retention");
    assert!(
        log.base_offset() > 0,
        "retention is still held on a shard with no follower"
    );
}
