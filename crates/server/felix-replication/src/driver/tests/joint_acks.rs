//! Acknowledging while a set change is pending: a record is held only when a
//! majority of the set being left holds it as well as a majority of the new
//! set (rule 8 of "Electing without the control plane"), and the leader
//! reports its generation-start record once that is so.

use felix_common::fleet::{FleetGate, GENERATION_START, JOINT_FENCE, MAJORITY_ACK};
use felix_router::PendingChange;
use felix_wire::internal::PeerCapabilities;

use super::quorum::quorum_leader;
use super::*;

/// Answers every batch for the nodes it was given and refuses the rest.
struct Answers {
    ok: &'static [&'static str],
}

impl PeerRequester for Answers {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let (InternalMessage::ReplicateRecords(batch)
        | InternalMessage::ReplicateMarkedRecords(batch)) = message
        else {
            panic!("the driver sent something other than a replication batch");
        };
        if !self.ok.contains(&node_id) {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "down".to_string(),
            });
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }

    fn recorded_capabilities(&self, _node_id: &str) -> Option<PeerCapabilities> {
        Some(PeerCapabilities::ELECTION)
    }

    fn offers_election(&self) -> bool {
        true
    }
}

fn marks(joint_fence: bool) -> QuorumMarks {
    let names: Vec<&str> = [GENERATION_START, MAJORITY_ACK]
        .iter()
        .chain(joint_fence.then_some(&JOINT_FENCE))
        .map(|feature| feature.name())
        .collect();
    let fleet = FleetGate::new(names.clone());
    fleet.observe(names);
    QuorumMarks::with_fleet(Arc::new(fleet))
}

/// The route of generation 4 led from here: followers `replicas`, and the
/// control plane's set `leaving` when a change is pending at generation 5.
fn route(replicas: &[&str], leaving: Option<&[&str]>) -> Arc<ShardRouter> {
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
            replicas: replicas.iter().map(|r| r.to_string()).collect(),
            generation: 4,
            draining: false,
            successor: None,
            routing: Default::default(),
            incarnation: 0,
            fence_storage_ids: Default::default(),
            pending: leaving.map(|leaving| PendingChange {
                generation: 5,
                leaving: leaving.iter().map(|node| node.to_string()).collect(),
            }),
        }],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

/// One pass over three records at generation 4: the mark it published, and
/// the pending set it reported as started.
async fn pass(
    ok: &'static [&'static str],
    router: &ShardRouter,
    marks: &QuorumMarks,
) -> (u64, Option<u64>) {
    let (broker, _dir) = quorum_leader(3, 4).await;
    let pass = replicate_once(
        &Answers { ok },
        &broker,
        router,
        marks,
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
    let started = pass.reports.first().and_then(|report| {
        assert_eq!(
            report.generation, 4,
            "the report names the route's generation"
        );
        report.joint_start_acked
    });
    (marks.offset(&watch_key(&key()), 4).unwrap_or(0), started)
}

const OLD: &[&str] = &[LOCAL, "broker-b", "broker-c"];

/// **Acknowledgements wait for both majorities.** The set moves from
/// {a, b, c} to {a, c, d}. `c` is in both sets, so its copy completes a
/// majority of each, and the start record is then reported.
#[tokio::test]
async fn a_pending_change_acknowledges_on_both_majorities() {
    let router = route(&["broker-c", "broker-d"], Some(OLD));

    let (mark, started) = pass(&["broker-c"], &router, &marks(true)).await;

    assert_eq!(mark, 3);
    assert_eq!(started, Some(5));
}

/// **A majority of the new set alone does not acknowledge.** `d` and the
/// leader are two of {a, c, d}, and only one of {a, b, c}. Nothing is
/// reported either: the start record is not held by the set being left.
#[tokio::test]
async fn a_new_set_majority_alone_does_not_acknowledge() {
    let router = route(&["broker-c", "broker-d"], Some(OLD));

    let (mark, started) = pass(&["broker-d"], &router, &marks(true)).await;

    assert_eq!(mark, 0);
    assert_eq!(started, None);
}

/// **Without the joint-fence feature the pending change is not read at all**:
/// the new set's majority acknowledges, as it did before.
#[tokio::test]
async fn without_joint_fence_a_pending_change_acknowledges_on_the_new_set_alone() {
    let router = route(&["broker-c", "broker-d"], Some(OLD));

    let (mark, started) = pass(&["broker-d"], &router, &marks(false)).await;

    assert_eq!(mark, 3);
    assert_eq!(started, None);
}

/// **After the flip only the new set counts.** The route no longer carries a
/// pending change, so `d` with the leader is enough.
#[tokio::test]
async fn after_the_flip_the_new_set_alone_acknowledges() {
    let router = route(&["broker-c", "broker-d"], None);

    let (mark, started) = pass(&["broker-d"], &router, &marks(true)).await;

    assert_eq!(mark, 3);
    assert_eq!(started, None);
}

/// **A leader that does not know the set being left acknowledges nothing.**
#[tokio::test]
async fn an_unknown_set_being_left_acknowledges_nothing() {
    let router = route(&["broker-c", "broker-d"], Some(&[]));

    let (mark, started) = pass(&["broker-c", "broker-d"], &router, &marks(true)).await;

    assert_eq!(mark, 0);
    assert_eq!(started, None);
}
