//! A follower's storage identity: a wiped copy starts over, a restarted one
//! keeps its place.

use std::sync::atomic::{AtomicBool, Ordering};

use felix_storage::StorageId;

use super::*;
use crate::FollowerCursor;
use crate::peer::{Incarnation, PeerIdentity};

fn id(byte: u8) -> StorageId {
    StorageId::from_bytes([byte; 16])
}

/// A follower that stores everything shipped to it, under a storage identity
/// the test can change. Once `busy` it answers every batch with a retryable
/// error, as one still opening its logs after a restart does.
#[derive(Default)]
struct IdentifiedFollower {
    inner: AcceptingFollower,
    identity: Mutex<PeerIdentity>,
    busy: AtomicBool,
}

impl IdentifiedFollower {
    fn with(identity: StorageId) -> Self {
        Self::at(PeerIdentity::storage(identity))
    }

    fn at(identity: PeerIdentity) -> Self {
        Self {
            identity: Mutex::new(identity),
            ..Self::default()
        }
    }

    fn restart_as(&self, identity: StorageId) {
        self.restart_at(PeerIdentity::storage(identity));
    }

    fn restart_at(&self, identity: PeerIdentity) {
        *self.identity.lock().expect("lock") = identity;
        self.busy.store(true, Ordering::SeqCst);
    }
}

impl PeerRequester for IdentifiedFollower {
    async fn request(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        if self.busy.load(Ordering::SeqCst) {
            return Ok(InternalMessage::ReplicateError(
                felix_wire::internal::ReplicateError {
                    correlation_id: message.correlation_id(),
                    code: felix_wire::internal::ErrorCode::Unavailable,
                    expected_offset: 0,
                    detail: "still opening".to_string(),
                },
            ));
        }
        self.inner.request(node_id, addr, message).await
    }

    async fn identity(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
    ) -> std::result::Result<PeerIdentity, PeerError> {
        Ok(*self.identity.lock().expect("lock"))
    }
}

async fn pass<R: PeerRequester + Sync>(
    follower: &R,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
    groups: &mut HashMap<ShardKey, ShardCursors>,
    rebuilds: &Rebuilds,
) -> ShardReport {
    let pass = replicate_once_with(
        follower,
        broker,
        router,
        &Unfenced,
        &crate::promotion::NoGate,
        &QuorumMarks::new(),
        None,
        cursors,
        groups,
        &mut HashMap::new(),
        &mut HashMap::new(),
        rebuilds,
        &MoveThrottle::unlimited(),
    )
    .await;
    pass.reports.last().expect("a report").clone()
}

/// **A wiped follower on an idle shard is not reported caught up (#1266).**
/// Nothing ships on an idle shard, so before the leader asked for the
/// identity it went on reporting the empty copy as holding everything, and
/// placement could promote it.
#[tokio::test]
async fn a_wiped_follower_on_an_idle_shard_is_not_reported_caught_up() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = IdentifiedFollower::with(id(1));
    let mut cursors = HashMap::new();
    let rebuilds = Rebuilds::disabled();

    let report = pass(
        &follower,
        &broker,
        &router,
        &mut cursors,
        &mut HashMap::new(),
        &rebuilds,
    )
    .await;
    assert_eq!(report.caught_up, vec!["broker-b".to_string()]);
    assert_eq!(report.storage_ids, vec![("broker-b".to_string(), id(1))]);

    follower.restart_as(id(2));
    let report = pass(
        &follower,
        &broker,
        &router,
        &mut cursors,
        &mut HashMap::new(),
        &rebuilds,
    )
    .await;
    assert!(
        report.caught_up.is_empty(),
        "a wiped copy was reported as holding the log: {report:?}",
    );
    assert_eq!(report.storage_ids, vec![("broker-b".to_string(), id(2))]);
}

/// **A follower that only restarted still counts**, before it has answered
/// anything: its identity is the one its position was learned under.
#[tokio::test]
async fn a_restarted_follower_still_counts() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = IdentifiedFollower::with(id(1));
    let mut cursors = HashMap::new();
    let rebuilds = Rebuilds::disabled();

    pass(
        &follower,
        &broker,
        &router,
        &mut cursors,
        &mut HashMap::new(),
        &rebuilds,
    )
    .await;
    follower.restart_as(id(1));
    let report = pass(
        &follower,
        &broker,
        &router,
        &mut cursors,
        &mut HashMap::new(),
        &rebuilds,
    )
    .await;
    assert_eq!(report.caught_up, vec!["broker-b".to_string()]);
    assert_eq!(report.storage_ids, vec![("broker-b".to_string(), id(1))]);
}

/// **A new incarnation starts the copy over even without a storage
/// identity.** A follower that sends none cannot be told from a wiped copy
/// of itself by its storage, so before the incarnation the leader went on
/// counting its old position across a restart.
#[tokio::test]
async fn a_new_incarnation_on_the_same_storage_starts_the_copy_over() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let at = |incarnation| PeerIdentity {
        storage: None,
        incarnation,
    };
    let follower = IdentifiedFollower::at(at(Incarnation::At(1)));
    let mut cursors = HashMap::new();
    let rebuilds = Rebuilds::disabled();

    let report = pass(
        &follower,
        &broker,
        &router,
        &mut cursors,
        &mut HashMap::new(),
        &rebuilds,
    )
    .await;
    assert_eq!(report.caught_up, vec!["broker-b".to_string()]);
    assert_eq!(
        report.node_incarnations,
        vec![("broker-b".to_string(), Some(1))]
    );

    follower.restart_at(at(Incarnation::Unregistered));
    let report = pass(
        &follower,
        &broker,
        &router,
        &mut cursors,
        &mut HashMap::new(),
        &rebuilds,
    )
    .await;
    assert!(
        report.caught_up.is_empty(),
        "a restarted process was counted on its predecessor's position: {report:?}",
    );
    assert_eq!(
        report.node_incarnations,
        vec![("broker-b".to_string(), None)]
    );
}

/// **A new identity resets the logs that ride the shard too, and gives back
/// a rebuild slot the old copy held.** The slot is otherwise only recounted
/// at the next pass, and the cursors would keep vouching for the old copy.
#[tokio::test]
async fn an_identity_change_resets_ride_along_cursors_and_frees_a_rebuild_slot() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = IdentifiedFollower::with(id(2));
    follower.busy.store(true, Ordering::SeqCst);
    let addr = nodes()["broker-b"].advertise_addr;

    let mut entry = ShardCursors::at(4);
    entry.followers.push(FollowerCursor {
        rebuilding: true,
        storage_id: Some(id(1)),
        ..FollowerCursor::new("broker-b", addr, 0)
    });
    let mut cursors = HashMap::from([(key(), entry)]);
    let mut group = ShardCursors::at(4);
    group.followers.push(FollowerCursor {
        next_offset: 5,
        confirmed: 5,
        stalled: false,
        storage_id: Some(id(1)),
        ..FollowerCursor::new("broker-b", addr, 0)
    });
    let mut groups = HashMap::from([(key(), group)]);
    let rebuilds = Rebuilds::new(crate::RebuildPolicy {
        max_concurrent: 1,
        bytes_per_sec: 0,
    });

    pass(
        &follower,
        &broker,
        &router,
        &mut cursors,
        &mut groups,
        &rebuilds,
    )
    .await;

    let cursor = &cursors[&key()].followers[0];
    assert!(!cursor.rebuilding);
    assert_eq!(cursor.storage_id, Some(id(2)));
    assert!(
        rebuilds.try_begin(),
        "the old copy's rebuild slot was not given back"
    );
    let group = &groups[&key()].followers[0];
    assert_eq!(
        (group.confirmed, group.next_offset, group.storage_id),
        (0, 0, Some(id(2))),
        "the group cursor still describes the old copy",
    );
}

/// **A follower that gives no identity is reported without one**, and
/// otherwise exactly as before.
#[tokio::test]
async fn a_follower_without_identity_is_reported_without_one() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let mut cursors = HashMap::new();

    let report = pass(
        &follower,
        &broker,
        &router,
        &mut cursors,
        &mut HashMap::new(),
        &Rebuilds::disabled(),
    )
    .await;
    assert_eq!(report.caught_up, vec!["broker-b".to_string()]);
    assert!(report.storage_ids.is_empty());
}

/// `broker-b` is down: it fails at once, probe and batch alike. `broker-c`
/// came back wiped under a new identity and never answers a batch.
struct WipedAndDown;

impl PeerRequester for WipedAndDown {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        _message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        if node_id == "broker-c" {
            std::future::pending::<()>().await;
        }
        Err(PeerError::Unavailable {
            node_id: node_id.to_string(),
            detail: "down".to_string(),
        })
    }

    async fn identity(
        &self,
        node_id: &str,
        _addr: SocketAddr,
    ) -> std::result::Result<PeerIdentity, PeerError> {
        if node_id == "broker-c" {
            return Ok(PeerIdentity::storage(id(2)));
        }
        Err(PeerError::Unavailable {
            node_id: node_id.to_string(),
            detail: "down".to_string(),
        })
    }
}

/// **A follower's old position does not count before its identity is
/// checked.** `broker-c` confirmed all five records before it was wiped;
/// `broker-b` holds two and is down. Its exchange is still under way when
/// `broker-b`'s failure lands, and counting `broker-c` at five there
/// acknowledged three records only the leader holds.
#[tokio::test(start_paused = true)]
async fn a_wiped_followers_old_position_does_not_move_the_mark() {
    let (broker, _dir) = super::quorum::quorum_leader(5, 4).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let marks = super::quorum::follower_ack_marks();
    let nodes = nodes();
    let at = |node: &str, confirmed: u64| FollowerCursor {
        next_offset: confirmed,
        confirmed,
        stalled: false,
        storage_id: Some(id(1)),
        ..FollowerCursor::new(node, nodes[node].advertise_addr, 0)
    };
    let mut entry = ShardCursors::at(4);
    entry.followers = vec![at("broker-b", 2), at("broker-c", 5)];
    let mut cursors = HashMap::from([(key(), entry)]);

    let (mut group, mut dead, mut counters) = (HashMap::new(), HashMap::new(), HashMap::new());
    let pass = replicate_once(
        &WipedAndDown,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut group,
        &mut dead,
        &mut counters,
    );
    // The pass waits on `broker-c` for good; the mark is decided before.
    let _ = tokio::time::timeout(Duration::from_secs(1), pass).await;

    let mark = marks.offset(&watch_key(&key()), 4).unwrap_or(0);
    assert!(
        mark <= 2,
        "acknowledged up to {mark} on a copy that was wiped",
    );
}

/// **Nor does its old position in the counter log**, which rides the same
/// exchange: the counter mark would release counter adds only the leader
/// holds.
#[tokio::test(start_paused = true)]
async fn a_wiped_followers_old_counter_position_does_not_move_the_counter_mark() {
    let (broker, _dir) = super::quorum::quorum_cache_leader(4).await;
    let cache = ShardKey {
        kind: felix_router::ShardKind::Cache,
        ..key()
    };
    let nodes = nodes();
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    router.publish(
        RoutingTable::build(
            [(
                cache.clone(),
                LOCAL.to_string(),
                vec!["broker-b".to_string(), "broker-c".to_string()],
                4,
            )],
            &nodes,
        ),
        &nodes,
    );
    let marks = super::quorum::fleet_marks(true);
    let at = |node: &str, confirmed: u64| FollowerCursor {
        next_offset: confirmed,
        confirmed,
        stalled: false,
        storage_id: Some(id(1)),
        ..FollowerCursor::new(node, nodes[node].advertise_addr, 0)
    };
    // `broker-c` held the whole cache log (two) and counter log (three).
    let seeded = |held: u64| {
        let mut entry = ShardCursors::at(4);
        entry.followers = vec![at("broker-b", 0), at("broker-c", held)];
        HashMap::from([(cache.clone(), entry)])
    };
    let (mut cursors, mut counters) = (seeded(2), seeded(3));
    let (mut group, mut dead) = (HashMap::new(), HashMap::new());

    let pass = replicate_once(
        &WipedAndDown,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut group,
        &mut dead,
        &mut counters,
    );
    let _ = tokio::time::timeout(Duration::from_secs(1), pass).await;

    let watched = watch_key(&cache);
    assert_eq!(marks.offset(&watched, 4).unwrap_or(0), 0);
    assert_eq!(
        marks.counters().offset(&watched, 4).unwrap_or(0),
        0,
        "counter adds acknowledged on a copy that was wiped",
    );
}

/// `broker-b` is down. `broker-c` crashed and came back wiped, but the
/// leader's pooled connection to the old process is still open: the probe
/// reads the old handshake's identity. A request either goes unanswered
/// there or, once the pool has dialled again, is answered by the new copy.
struct StaleConnection {
    answered_by_new_copy: bool,
}

impl PeerRequester for StaleConnection {
    async fn request(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        self.request_identified(node_id, addr, message)
            .await
            .map(|(answer, _)| answer)
    }

    async fn identity(
        &self,
        node_id: &str,
        _addr: SocketAddr,
    ) -> std::result::Result<PeerIdentity, PeerError> {
        if node_id == "broker-c" {
            return Ok(PeerIdentity::storage(id(1)));
        }
        Err(PeerError::Unavailable {
            node_id: node_id.to_string(),
            detail: "down".to_string(),
        })
    }

    async fn request_identified(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<(InternalMessage, PeerIdentity), PeerError> {
        if node_id != "broker-c" {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "down".to_string(),
            });
        }
        if !self.answered_by_new_copy {
            std::future::pending::<()>().await;
        }
        // Whatever the new copy says, even that it stored the batch, it is not
        // the copy that confirmed the cursor's position.
        let (InternalMessage::ReplicateRecords(batch)
        | InternalMessage::ReplicateMarkedRecords(batch)) = message
        else {
            panic!("the driver sent something other than a replication batch");
        };
        let answer = InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        });
        Ok((answer, PeerIdentity::storage(id(2))))
    }
}

/// **The probe alone does not vouch for a follower.** It reads the identity
/// of the pooled connection's handshake, which names the old copy until QUIC
/// notices the old process is gone. Only an answer counts, and only one from
/// the copy the cursor describes.
#[tokio::test(start_paused = true)]
async fn a_stale_connections_identity_does_not_count_a_wiped_follower() {
    for answered_by_new_copy in [false, true] {
        let (broker, _dir) = super::quorum::quorum_leader(5, 4).await;
        let router = router(LOCAL, &["broker-b", "broker-c"], 4);
        let marks = super::quorum::follower_ack_marks();
        let nodes = nodes();
        let at = |node: &str, confirmed: u64| FollowerCursor {
            next_offset: confirmed,
            confirmed,
            stalled: false,
            storage_id: Some(id(1)),
            ..FollowerCursor::new(node, nodes[node].advertise_addr, 0)
        };
        let mut entry = ShardCursors::at(4);
        entry.followers = vec![at("broker-b", 2), at("broker-c", 5)];
        let mut cursors = HashMap::from([(key(), entry)]);

        let (mut group, mut dead, mut counters) = (HashMap::new(), HashMap::new(), HashMap::new());
        let follower = StaleConnection {
            answered_by_new_copy,
        };
        let pass = replicate_once(
            &follower,
            &broker,
            &router,
            &marks,
            None,
            &mut cursors,
            &mut group,
            &mut dead,
            &mut counters,
        );
        let _ = tokio::time::timeout(Duration::from_secs(1), pass).await;

        let mark = marks.offset(&watch_key(&key()), 4).unwrap_or(0);
        assert!(
            mark <= 2,
            "acknowledged up to {mark} on a copy that was wiped \
             (answered by the new copy: {answered_by_new_copy})",
        );
    }
}

/// Stores every batch, of any log, under identity 1 and keeps who was asked;
/// `down` answers nothing.
struct CountingFollower {
    down: &'static str,
    asked: Mutex<Vec<String>>,
}

impl CountingFollower {
    fn new(down: &'static str) -> Self {
        Self {
            down,
            asked: Mutex::new(Vec::new()),
        }
    }

    fn asked(&self, node: &str) -> usize {
        self.asked
            .lock()
            .expect("lock")
            .iter()
            .filter(|asked| *asked == node)
            .count()
    }
}

impl PeerRequester for CountingFollower {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        if node_id == self.down {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "down".to_string(),
            });
        }
        self.asked.lock().expect("lock").push(node_id.to_string());
        let (InternalMessage::ReplicateRecords(batch)
        | InternalMessage::ReplicateMarkedRecords(batch)
        | InternalMessage::ReplicateCacheRecords(batch)
        | InternalMessage::ReplicateCounterRecords(batch)
        | InternalMessage::ReplicateGroupRecords(batch)
        | InternalMessage::ReplicateDeadLetterRecords(batch)) = message
        else {
            panic!("the driver sent something other than a replication batch");
        };
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }

    async fn identity(
        &self,
        node_id: &str,
        _addr: SocketAddr,
    ) -> std::result::Result<PeerIdentity, PeerError> {
        if node_id == self.down {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "down".to_string(),
            });
        }
        Ok(PeerIdentity::storage(id(1)))
    }
}

/// Cursors for `key` at generation 4: `broker-b` down at `b`, `broker-c` level
/// at `c`, both learned under identity 1.
fn level_cursors(key: &ShardKey, b: u64, c: u64) -> HashMap<ShardKey, ShardCursors> {
    let nodes = nodes();
    let at = |node: &str, confirmed: u64, stalled: bool| FollowerCursor {
        next_offset: confirmed,
        confirmed,
        stalled,
        storage_id: Some(id(1)),
        ..FollowerCursor::new(node, nodes[node].advertise_addr, 0)
    };
    let mut entry = ShardCursors::at(4);
    entry.followers = vec![at("broker-b", b, true), at("broker-c", c, false)];
    HashMap::from([(key.clone(), entry)])
}

/// **A level follower whose answer the mark needs is asked for one.**
/// `broker-c` confirmed everything in an earlier pass whose mark did not
/// count it; nothing is left to ship, so without its last record going again
/// the mark would wait for the next write.
#[tokio::test(start_paused = true)]
async fn a_level_follower_the_mark_needs_is_sent_its_last_record_again() {
    let (broker, _dir) = super::quorum::quorum_leader(5, 4).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let marks = super::quorum::follower_ack_marks();
    let follower = CountingFollower::new("broker-b");
    let mut cursors = level_cursors(&key(), 0, 5);
    let (mut group, mut dead, mut counters) = (HashMap::new(), HashMap::new(), HashMap::new());

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
    )
    .await;

    assert_eq!(follower.asked("broker-c"), 1);
    assert_eq!(marks.offset(&watch_key(&key()), 4), Some(5));
}

/// **A deposed leader asks no level follower again**: it cannot count
/// itself, so `broker-c` alone, with `broker-b` down, raises nothing.
#[tokio::test(start_paused = true)]
async fn a_level_follower_that_cannot_raise_the_mark_is_not_sent_anything() {
    let (broker, _dir) = super::quorum::quorum_leader(5, 4).await;
    broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .accept_generation(5, None)
        .await
        .expect("accept a newer leader");
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let marks = super::quorum::follower_ack_marks();
    let follower = CountingFollower::new("broker-b");
    let mut cursors = level_cursors(&key(), 0, 5);
    let (mut group, mut dead, mut counters) = (HashMap::new(), HashMap::new(), HashMap::new());

    for _ in 0..3 {
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
        )
        .await;
    }

    assert_eq!(follower.asked("broker-c"), 0);
}

/// **An idle cache that does not acknowledge at a quorum resends nothing.**
/// Its counter mark is never published, so a check against it alone would
/// send the last counter record every pass.
#[tokio::test(start_paused = true)]
async fn an_idle_cache_below_quorum_resends_nothing() {
    let (broker, _dir) =
        super::quorum::cache_leader(4, felix_broker::ConsistencyLevel::Leader).await;
    let cache = ShardKey {
        kind: felix_router::ShardKind::Cache,
        ..key()
    };
    let nodes = nodes();
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    router.publish(
        RoutingTable::build(
            [(
                cache.clone(),
                LOCAL.to_string(),
                vec!["broker-b".to_string(), "broker-c".to_string()],
                4,
            )],
            &nodes,
        ),
        &nodes,
    );
    let marks = super::quorum::fleet_marks(true);
    let follower = CountingFollower::new("broker-b");
    let (mut cursors, mut counters) = (level_cursors(&cache, 0, 2), level_cursors(&cache, 0, 3));
    let (mut group, mut dead) = (HashMap::new(), HashMap::new());

    for _ in 0..3 {
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
        )
        .await;
    }

    assert_eq!(follower.asked("broker-c"), 0);
}

/// **A drain does not hand the shard to a wiped successor.** `broker-c`, the
/// successor, confirmed everything before it was wiped; the pooled
/// connection still gives its old identity. Judged by its old position the
/// drain would report it level, and the control plane would cut over to an
/// empty copy.
#[tokio::test(start_paused = true)]
async fn a_drain_does_not_hand_over_to_a_wiped_successor() {
    for answered_by_new_copy in [false, true] {
        let (broker, _dir) = super::quorum::quorum_leader(5, 4).await;
        let router = draining_router(LOCAL, &["broker-c", "broker-b"], 4);
        let marks = super::quorum::follower_ack_marks();
        let follower = StaleConnection {
            answered_by_new_copy,
        };
        let mut cursors = level_cursors(&key(), 0, 5);
        let (mut group, mut dead, mut counters) = (HashMap::new(), HashMap::new(), HashMap::new());

        let pass = replicate_once(
            &follower,
            &broker,
            &router,
            &marks,
            None,
            &mut cursors,
            &mut group,
            &mut dead,
            &mut counters,
        );
        // Unanswered, the drain waits on the successor for good.
        if let Ok(pass) = tokio::time::timeout(Duration::from_secs(1), pass).await {
            assert!(
                pass.reports.iter().all(|report| !report.drained),
                "drained to a wiped successor: {:?}",
                pass.reports,
            );
        } else {
            assert!(!answered_by_new_copy, "the pass did not finish");
        }
        assert_eq!(marks.offset(&watch_key(&key()), 4).unwrap_or(0), 0);
    }
}

/// **A drain on an idle shard still completes**: the successor is level and
/// sent nothing, so it is asked once for an answer that shows it.
#[tokio::test(start_paused = true)]
async fn a_drain_on_an_idle_shard_hands_over_to_a_level_successor() {
    // Not `Quorum`, so nothing else asks the successor for an answer.
    let (broker, _dir) = leader_with(5).await;
    let router = draining_router(LOCAL, &["broker-c", "broker-b"], 4);
    let marks = super::quorum::follower_ack_marks();
    let follower = CountingFollower::new("broker-b");
    let mut cursors = level_cursors(&key(), 0, 5);
    let (mut group, mut dead, mut counters) = (HashMap::new(), HashMap::new(), HashMap::new());

    let pass = replicate_once(
        &follower,
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

    assert!(
        pass.reports.iter().any(|report| report.drained),
        "a level successor was not handed the shard: {:?}",
        pass.reports,
    );
    assert_eq!(follower.asked("broker-c"), 1);
}
