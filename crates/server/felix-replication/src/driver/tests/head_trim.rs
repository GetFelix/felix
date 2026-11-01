//! Head trims across a failover (#1662): a trim ships with its floor, only to
//! a follower that reads it, and a promoted follower serves nothing below one
//! a majority acknowledged.

use std::collections::BTreeMap;

use felix_storage::log::RecordMark;
use felix_wire::internal::PeerCapabilities;

use super::*;
use crate::replica::ReplicaHandler;

const FLOOR: u64 = 3;

/// A router as `local` sees it.
fn router_at(local: &str, leader: &str, replicas: &[&str], generation: u64) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        local,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    publish(&router, leader, replicas, generation);
    router
}

/// A broker with the stream registered as durable and `Quorum`.
pub(super) async fn quorum_broker() -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            segment_size_bytes: 4 * 1024,
            index_spacing_bytes: 256,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
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
    (broker, dir)
}

pub(super) async fn shard_log(broker: &Broker) -> felix_broker::StreamLog {
    broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
}

/// Append a trim record asking that reads below `floor` be refused.
async fn append_trim(log: &felix_broker::StreamLog, floor: u64) -> u64 {
    let tail = log.tail_offset().await.expect("tail");
    let pending = log
        .begin_append_marked_at(
            tail,
            &[Bytes::from_static(b"tag")],
            &[RecordMark::Trim { floor }],
            &[],
            &[],
        )
        .await
        .expect("append")
        .expect("at the tail");
    log.commit(&pending).await.expect("commit");
    tail
}

/// Every record a log holds, trims included, as replication reads it.
async fn records(log: &felix_broker::StreamLog) -> Vec<(u64, Bytes, RecordMark)> {
    log.read_replicated_from(log.physical_base(), 1 << 20)
        .await
        .expect("read")
        .into_iter()
        .map(|record| (record.offset, record.payload, record.mark))
        .collect()
}

/// Real replicas, reached as the pool reaches them: a batch holding a trim
/// goes only to one that offered `HEAD_TRIM`.
struct Replicas {
    replicas: Map<String, (ReplicaHandler, PeerCapabilities)>,
    leader: Mutex<String>,
    sent_a_trim: Mutex<Vec<String>>,
}

impl PeerRequester for Replicas {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let Some((handler, offered)) = self.replicas.get(node_id) else {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "down".to_string(),
            });
        };
        if let Some(needed) = crate::peer::pool::required_capability(&message)
            && !offered.contains(needed)
        {
            return Err(PeerError::Unsupported {
                node_id: node_id.to_string(),
                kind: message.kind(),
            });
        }
        let leader = self.leader.lock().expect("lock").clone();
        match message {
            InternalMessage::ReplicateRecords(batch)
            | InternalMessage::ReplicateMarkedRecords(batch) => {
                if batch.holds_trim() {
                    self.sent_a_trim
                        .lock()
                        .expect("lock")
                        .push(node_id.to_string());
                }
                Ok(handler
                    .apply(Some(&leader), batch, felix_broker::LogKind::Stream)
                    .await)
            }
            other => panic!("unexpected {:?}", other.kind()),
        }
    }

    fn recorded_capabilities(&self, node_id: &str) -> Option<PeerCapabilities> {
        self.replicas.get(node_id).map(|(_, offered)| *offered)
    }
}

/// Run replication passes for the leader `broker` until `until` holds, at
/// most a few times.
async fn passes(
    replicas: &Replicas,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    marks: &QuorumMarks,
    until: impl AsyncFn() -> bool,
) {
    let (mut cursors, mut group, mut dead, mut counters) = (
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    );
    for _ in 0..8 {
        replicate_once_with(
            replicas,
            broker,
            router,
            &Unfenced,
            &crate::promotion::NoGate,
            marks,
            None,
            &mut cursors,
            &mut group,
            &mut dead,
            &mut counters,
            &Rebuilds::disabled(),
            &MoveThrottle::unlimited(),
        )
        .await;
        if until().await {
            return;
        }
    }
    panic!("replication did not get there");
}

/// **A trim acknowledged before its leader dies holds on the follower
/// promoted in its place.**
///
/// The leader ships six records and a trim to floor 3 and the mark passes
/// the trim, so a client was told it took. The followers hold the trim but
/// have not learned that it committed, so neither applies its floor. The
/// leader then dies and a follower is promoted. Once its mark covers what it
/// inherited, it refuses reads below the floor, serves everything at and
/// above it, and its copy matches the surviving follower's record for
/// record. A follower that never offered `HEAD_TRIM` is sent no trim at any
/// point.
#[tokio::test]
async fn an_acknowledged_trim_survives_the_leaders_death() {
    let (a, _a_dir) = quorum_broker().await;
    let (b, _b_dir) = quorum_broker().await;
    let (c, _c_dir) = quorum_broker().await;
    let (d, _d_dir) = quorum_broker().await;
    let at_4 = ["broker-b", "broker-c", "broker-d"];
    let b_view = router_at("broker-b", "broker-a", &at_4, 4);
    let c_view = router_at("broker-c", "broker-a", &at_4, 4);
    let d_view = router_at("broker-d", "broker-a", &at_4, 4);
    let reads_trims = PeerCapabilities::HEAD_TRIM;
    let replicas = Replicas {
        replicas: [
            (
                "broker-b",
                ReplicaHandler::new(Arc::clone(&b), Arc::clone(&b_view)),
                reads_trims,
            ),
            (
                "broker-c",
                ReplicaHandler::new(Arc::clone(&c), Arc::clone(&c_view)),
                reads_trims,
            ),
            (
                "broker-d",
                ReplicaHandler::new(Arc::clone(&d), Arc::clone(&d_view)),
                PeerCapabilities::NONE,
            ),
        ]
        .into_iter()
        .map(|(node, handler, offered)| (node.to_string(), (handler, offered)))
        .collect(),
        leader: Mutex::new("broker-a".to_string()),
        sent_a_trim: Mutex::new(Vec::new()),
    };

    // Generation 4, led by broker-a.
    let a_log = shard_log(&a).await;
    a_log.record_generation(4, 0).expect("generation");
    for i in 0..6 {
        a_log
            .append(&[Bytes::from(format!("v{i}"))])
            .await
            .expect("append");
    }
    let trim_at = append_trim(&a_log, FLOOR).await;
    let a_marks = QuorumMarks::new();
    let a_router = router_at(LOCAL, "broker-a", &at_4, 4);
    passes(&replicas, &a, &a_router, &a_marks, async || {
        a_marks.offset(&watch_key(&key()), 4) == Some(trim_at + 1)
    })
    .await;
    assert_eq!(a_log.readable_base(), FLOOR, "the leader applied its trim");
    let (b_log, c_log, d_log) = (
        shard_log(&b).await,
        shard_log(&c).await,
        shard_log(&d).await,
    );
    for (name, log) in [("b", &b_log), ("c", &c_log)] {
        assert_eq!(
            log.tail_offset().await.expect("tail"),
            trim_at + 1,
            "{name}"
        );
        assert_eq!(
            log.readable_base(),
            0,
            "{name} applied a floor its commit offset has not passed"
        );
    }
    assert_eq!(d_log.tail_offset().await.expect("tail"), 0);

    // broker-a dies. broker-b takes generation 5 and writes a record of its
    // own, which its mark must pass before it serves.
    let at_5 = ["broker-c", "broker-d"];
    for view in [&b_view, &c_view, &d_view] {
        publish(view, "broker-b", &at_5, 5);
    }
    *replicas.leader.lock().expect("lock") = "broker-b".to_string();
    let promoted = Replicas {
        replicas: replicas
            .replicas
            .into_iter()
            .filter(|(node, _)| node != "broker-b")
            .collect(),
        leader: replicas.leader,
        sent_a_trim: replicas.sent_a_trim,
    };
    b_log.record_generation(5, trim_at + 1).expect("generation");
    b_log
        .append(&[Bytes::from_static(b"after")])
        .await
        .expect("append");
    let b_marks = QuorumMarks::new();
    passes(&promoted, &b, &b_view, &b_marks, async || {
        b_marks.offset(&watch_key(&key()), 5) == Some(trim_at + 2)
    })
    .await;
    assert_eq!(b_log.readable_base(), FLOOR);
    // A follower learns the commit offset from the next batch, and applies
    // the floor then.
    b_log
        .append(&[Bytes::from_static(b"later")])
        .await
        .expect("append");
    passes(&promoted, &b, &b_view, &b_marks, async || {
        c_log.tail_offset().await.expect("tail") == trim_at + 3
    })
    .await;
    assert_eq!(c_log.readable_base(), FLOOR);

    assert!(
        matches!(
            b_log.read_from(0, 1 << 20).await,
            Err(felix_broker::BrokerError::CursorTooOld { oldest: FLOOR, .. })
        ),
        "the promoted leader served a record below the acknowledged floor"
    );
    let served: Vec<String> = b_log
        .read_from(FLOOR, 1 << 20)
        .await
        .expect("read")
        .into_iter()
        .map(|record| String::from_utf8(record.payload.to_vec()).expect("utf8"))
        .collect();
    assert_eq!(served, ["v3", "v4", "v5", "after", "later"]);
    assert_eq!(
        records(&b_log).await,
        records(&c_log).await,
        "the surviving copies diverged"
    );
    assert_eq!(
        records(&a_log).await[..],
        records(&b_log).await[..=trim_at as usize],
        "the promoted copy is not the one the dead leader acknowledged"
    );
    assert!(
        !promoted
            .sent_a_trim
            .lock()
            .expect("lock")
            .contains(&"broker-d".to_string()),
        "a follower without HEAD_TRIM was sent a trim"
    );
    assert!(d_log.tail_offset().await.expect("tail") <= trim_at);
}

/// **The mark lets readers in only once an inherited trim's floor applies.**
///
/// Readers of a `Quorum` stream stop at the mark, and a promoted leader's
/// log applies a trim's floor only below its own commit offset, which the
/// driver used to raise after publishing the mark. A reader let in between
/// was served below the floor of a trim the old leader acknowledged.
#[tokio::test]
async fn the_mark_opens_reads_only_once_an_inherited_trim_applies() {
    for by_followers in [true, false] {
        let (broker, _dir) = quorum_broker().await;
        let log = shard_log(&broker).await;
        log.hold_retention_at_commit(true).await.expect("hold");
        log.record_generation(4, 0).expect("generation");
        for i in 0..6 {
            log.append(&[Bytes::from(format!("v{i}"))])
                .await
                .expect("append");
        }
        let trim_at = append_trim(&log, FLOOR).await;
        log.record_generation(5, trim_at + 1).expect("generation");
        log.append(&[Bytes::from_static(b"after")])
            .await
            .expect("append");
        assert_eq!(log.readable_base(), 0, "nothing committed yet");

        let marks = QuorumMarks::new();
        let mark = trim_at + 2;
        let report =
            crate::reporter::shard_report(&key(), 5, mark, mark, &[], &BTreeMap::new(), false);
        let published = publish_mark(
            &broker,
            None,
            &marks,
            &key(),
            0,
            5,
            &report,
            mark,
            by_followers,
            Some(&log),
        )
        .await;
        assert!(published);
        assert_eq!(marks.offset(&watch_key(&key()), 5), Some(mark));
        assert_eq!(
            log.readable_base(),
            FLOOR,
            "the mark opened reads below an inherited trim (by followers: {by_followers})"
        );
    }
}
