//! What a promoted leader says of followers it has not reached yet: the
//! identity its promotion named them by (#1574), and that none of them holds
//! the log it inherited (#1922).

use std::sync::atomic::{AtomicBool, Ordering};

use felix_storage::StorageId;
use felix_wire::internal::FenceOk;

use super::*;
use crate::peer::PeerIdentity;

fn id(byte: u8) -> StorageId {
    StorageId::from_bytes([byte; 16])
}

/// The failover that promoted this broker at generation 4 named broker-b by
/// `id(1)` and broker-c by `id(3)`.
fn promoted_router() -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    promote(&router, 4, &[("broker-b", id(1)), ("broker-c", id(3))]);
    router
}

/// Route a promotion of this broker at `generation` naming `ids`.
fn promote(router: &ShardRouter, generation: u64, ids: &[(&str, StorageId)]) {
    let nodes = nodes();
    let table = RoutingTable::build_with(
        [felix_router::Placed {
            key: key(),
            leader: LOCAL.to_string(),
            replicas: vec!["broker-b".to_string(), "broker-c".to_string()],
            generation,
            draining: false,
            successor: None,
            routing: Default::default(),
            incarnation: 0,
            fence_storage_ids: ids
                .iter()
                .map(|(node, id)| (node.to_string(), id.to_hex()))
                .collect(),
            member_storage_ids: Default::default(),
            pending: None,
        }],
        &nodes,
    );
    router.publish(table, &nodes);
}

/// broker-b was wiped and answers under `id(2)`, so its fence answer does not
/// count. broker-c answers under the `id(3)` it was named by, only while
/// `c_up`, and broker-b only until `b_down`. Both take the fence with an
/// empty log and store every batch.
#[derive(Default)]
struct PromotedFollowers {
    c_up: AtomicBool,
    b_down: AtomicBool,
}

impl PromotedFollowers {
    fn unreachable(&self, node_id: &str) -> Option<PeerError> {
        let down = match node_id {
            "broker-b" => self.b_down.load(Ordering::SeqCst),
            "broker-c" => !self.c_up.load(Ordering::SeqCst),
            _ => false,
        };
        down.then(|| PeerError::Unavailable {
            node_id: node_id.to_string(),
            detail: "paused".to_string(),
        })
    }
}

impl PeerRequester for PromotedFollowers {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        if let Some(err) = self.unreachable(node_id) {
            return Err(err);
        }
        match message {
            InternalMessage::Fence(fence) => Ok(InternalMessage::FenceOk(FenceOk {
                correlation_id: fence.correlation_id,
                log_end: 0,
                commit_offset: 0,
                last_generation: 0,
            })),
            InternalMessage::ReplicateRecords(batch) => {
                Ok(InternalMessage::ReplicateOk(ReplicateOk {
                    correlation_id: 0,
                    durable_offset: batch.first_offset + batch.payloads.len() as u64,
                    learner: false,
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

    async fn identity(
        &self,
        node_id: &str,
        _addr: SocketAddr,
    ) -> std::result::Result<PeerIdentity, PeerError> {
        if let Some(err) = self.unreachable(node_id) {
            return Err(err);
        }
        Ok(PeerIdentity::storage(match node_id {
            "broker-b" => id(2),
            _ => id(3),
        }))
    }
}

/// Promoted at generation 4 until opened.
struct Awaiting {
    closed: AtomicBool,
}

#[async_trait::async_trait]
impl crate::promotion::PromotionGate for Awaiting {
    fn awaiting(&self, _key: &crate::ShardKey) -> Option<u64> {
        self.closed.load(Ordering::SeqCst).then_some(4)
    }

    async fn open(&self, _key: &crate::ShardKey, _generation: u64) -> bool {
        self.closed.store(false, Ordering::SeqCst);
        true
    }
}

async fn pass(
    followers: &PromotedFollowers,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    gate: &Awaiting,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
) -> Pass {
    replicate_once_with(
        followers,
        broker,
        router,
        &Unfenced,
        gate,
        &QuorumMarks::new(),
        None,
        cursors,
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
        &Rebuilds::disabled(),
        &MoveThrottle::unlimited(),
    )
    .await
}

/// **A leader still fencing reports nothing, so no identity an uncounted
/// fence answer gave reaches the control plane.** broker-b's answer from
/// another copy than its promotion named is not counted, and while the fence
/// waits no report names broker-b by it. Were one stored, the next failover
/// would name broker-b by a copy no leader ever counted, and count it.
///
/// Once open, a follower this leader has not reached is named by the
/// promotion's identity, and one it has reached by the copy it answers from.
#[tokio::test]
async fn a_leader_still_fencing_reports_no_identity_from_its_fence_answers() {
    let (broker, _dir) = leader_with(3).await;
    let router = promoted_router();
    let followers = PromotedFollowers::default();
    let gate = Awaiting {
        closed: AtomicBool::new(true),
    };
    let mut cursors = HashMap::default();

    // broker-b's answer does not count and broker-c is away: no majority.
    let waiting = pass(&followers, &broker, &router, &gate, &mut cursors).await;
    assert!(waiting.fencing, "opened without a majority");
    assert!(
        waiting.reports.is_empty(),
        "reported while fencing: {:?}",
        waiting.reports
    );
    assert!(cursors.is_empty(), "cursors before the fence");

    // broker-c's answer counts, and the shard opens.
    followers.c_up.store(true, Ordering::SeqCst);
    let opening = pass(&followers, &broker, &router, &gate, &mut cursors).await;
    assert!(!opening.fencing, "the fence did not open the shard");
    assert!(opening.reports.is_empty(), "{:?}", opening.reports);

    // broker-c goes away again before this leader reaches it.
    followers.c_up.store(false, Ordering::SeqCst);
    let open = pass(&followers, &broker, &router, &gate, &mut cursors).await;
    let report = open.reports.last().expect("a report once open");
    assert_eq!(report.storage_ids, vec![("broker-b".to_string(), id(2))]);
    assert_eq!(
        report.inherited_storage,
        vec![("broker-c".to_string(), id(3).to_hex())],
        "{report:?}"
    );
}

/// **A set change that keeps the leader keeps what it named its followers
/// by** (#1922). At generation 5 this leader reaches neither follower, and a
/// report naming them by none would leave a failover unable to count either
/// one's fence answer. broker-b is named by the copy it answered from at
/// generation 4, and broker-c, never reached, by its promotion's identity.
#[tokio::test]
async fn a_set_change_keeps_the_names_its_leader_reported() {
    let (broker, _dir) = leader_with(3).await;
    let router = promoted_router();
    let followers = PromotedFollowers::default();
    let gate = Awaiting {
        closed: AtomicBool::new(false),
    };
    let mut cursors = HashMap::default();

    let promoted = pass(&followers, &broker, &router, &gate, &mut cursors).await;
    let report = promoted.reports.last().expect("a report");
    assert_eq!(report.storage_ids, vec![("broker-b".to_string(), id(2))]);
    assert_eq!(
        report.inherited_storage,
        vec![("broker-c".to_string(), id(3).to_hex())]
    );

    followers.b_down.store(true, Ordering::SeqCst);
    publish(&router, LOCAL, &["broker-b", "broker-c"], 5);
    let later = pass(&followers, &broker, &router, &gate, &mut cursors).await;
    let report = later.reports.last().expect("a report");
    assert_eq!(report.generation, 5);
    assert!(report.storage_ids.is_empty(), "{report:?}");
    assert_eq!(
        report.inherited_storage,
        vec![
            ("broker-b".to_string(), id(2).to_hex()),
            ("broker-c".to_string(), id(3).to_hex()),
        ],
        "{report:?}"
    );
}

/// **A promotion names the copies itself.** A later promotion of the same
/// leader that names broker-c by `id(4)` replaces what this leader named it
/// by before; a member it names no identity for is named by none.
#[tokio::test]
async fn a_promotion_replaces_the_names_its_leader_reported() {
    let (broker, _dir) = leader_with(3).await;
    let router = promoted_router();
    let followers = PromotedFollowers::default();
    let gate = Awaiting {
        closed: AtomicBool::new(false),
    };
    let mut cursors = HashMap::default();
    pass(&followers, &broker, &router, &gate, &mut cursors).await;

    followers.b_down.store(true, Ordering::SeqCst);
    promote(&router, 6, &[("broker-c", id(4))]);
    let later = pass(&followers, &broker, &router, &gate, &mut cursors).await;
    let report = later.reports.last().expect("a report");
    assert_eq!(report.generation, 6);
    assert_eq!(
        report.inherited_storage,
        vec![("broker-c".to_string(), id(4).to_hex())],
        "{report:?}"
    );
}

/// Marks as a fleet that finalized acknowledging `Quorum` streams and caches
/// by their followers' answers.
fn by_followers() -> QuorumMarks {
    use felix_common::fleet::{FENCED_CACHES, FleetGate, GENERATION_START, MAJORITY_ACK};
    let names = [
        GENERATION_START.name(),
        MAJORITY_ACK.name(),
        FENCED_CACHES.name(),
    ];
    let fleet = FleetGate::new(names);
    fleet.observe(names);
    QuorumMarks::with_fleet(Arc::new(fleet))
}

/// A `Quorum` leader promoted at generation 3 over 2 records an earlier
/// leader wrote, as after a failover.
async fn inheriting_at_3() -> (Arc<Broker>, TempDir) {
    let (broker, dir) =
        super::reports::registered_leader(2, felix_broker::ConsistencyLevel::Quorum).await;
    broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .record_generation(3, 2)
        .expect("record");
    (broker, dir)
}

/// **A leader acknowledging by its followers names none of them before they
/// hold what it inherited (#1922).** The earlier leader may have acknowledged
/// both records on a majority this leader's set no longer contains, as when a
/// failover swapped a member out. Named at offset 0, an empty follower could
/// be promoted next and open on a fence answered only by brokers that never
/// held them, losing an acknowledged write.
#[tokio::test]
async fn a_leader_acknowledging_by_followers_names_none_before_they_answer() {
    let (broker, _dir) = inheriting_at_3().await;
    let marks = by_followers();
    assert!(marks.serving().streams_by_followers);
    let requester = UnreachableFollowers {
        handshake: std::time::Duration::from_millis(20),
    };

    let pass = replicate_once(
        &requester,
        &broker,
        &router(LOCAL, &["broker-b", "broker-c"], 3),
        &marks,
        None,
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
    )
    .await;

    let report = pass.reports.last().expect("a report");
    assert!(
        report.caught_up.is_empty(),
        "followers holding none of the inherited log were offered for promotion: {:?}",
        report.caught_up,
    );
}

/// The same leader names a follower once it answers holding the inherited
/// log, so placement has a candidate again.
#[tokio::test]
async fn a_leader_acknowledging_by_followers_names_one_holding_what_it_inherited() {
    let (broker, _dir) = inheriting_at_3().await;

    let pass = replicate_once(
        &AcceptingFollower::default(),
        &broker,
        &router(LOCAL, &["broker-b"], 3),
        &by_followers(),
        None,
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
    )
    .await;

    let report = pass.reports.last().expect("a report");
    assert_eq!(report.caught_up, vec!["broker-b".to_string()]);
}
