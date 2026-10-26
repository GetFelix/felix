//! Naming a follower this leader has not reached by the identity its
//! promotion was decided on (#1574).

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
    let nodes = nodes();
    let table = RoutingTable::build_with(
        [felix_router::Placed {
            key: key(),
            leader: LOCAL.to_string(),
            replicas: vec!["broker-b".to_string(), "broker-c".to_string()],
            generation: 4,
            draining: false,
            successor: None,
            routing: Default::default(),
            incarnation: 0,
            fence_storage_ids: [
                ("broker-b".to_string(), id(1).to_hex()),
                ("broker-c".to_string(), id(3).to_hex()),
            ]
            .into(),
        }],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

/// broker-b was wiped and answers under `id(2)`, so its fence answer does not
/// count. broker-c answers under the `id(3)` it was named by, only while
/// `c_up`. Both take the fence with an empty log and store every batch.
#[derive(Default)]
struct PromotedFollowers {
    c_up: AtomicBool,
}

impl PromotedFollowers {
    fn unreachable(&self, node_id: &str) -> Option<PeerError> {
        (node_id == "broker-c" && !self.c_up.load(Ordering::SeqCst)).then(|| {
            PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "paused".to_string(),
            }
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
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
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
    let mut cursors = HashMap::new();

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

/// **A leader's later generation names nothing by inheritance**: only the
/// promotion's own assignment carries identities, and a later one at a new
/// generation carries none.
#[tokio::test]
async fn a_later_generation_inherits_no_names() {
    let (broker, _dir) = leader_with(3).await;
    let router = promoted_router();
    let followers = PromotedFollowers::default();
    let gate = Awaiting {
        closed: AtomicBool::new(false),
    };
    let mut cursors = HashMap::new();

    let promoted = pass(&followers, &broker, &router, &gate, &mut cursors).await;
    let report = promoted.reports.last().expect("a report");
    assert_eq!(
        report.inherited_storage,
        vec![("broker-c".to_string(), id(3).to_hex())]
    );

    publish(&router, LOCAL, &["broker-b", "broker-c"], 5);
    let later = pass(&followers, &broker, &router, &gate, &mut cursors).await;
    let report = later.reports.last().expect("a report");
    assert_eq!(report.generation, 5);
    assert!(report.inherited_storage.is_empty(), "{report:?}");
}
