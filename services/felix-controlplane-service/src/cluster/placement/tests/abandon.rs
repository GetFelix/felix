//! An operator giving up a shard's unreachable log: allowed only where
//! placement is holding the shard for it, and nowhere a lossless option exists.
use super::*;
use crate::store::ControlPlaneStore;

struct CaughtUpNodes(BTreeSet<String>);

impl CaughtUp for CaughtUpNodes {
    fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
        self.0.contains(node_id)
    }
}

fn catalog<'a>(
    streams: &'a [Stream],
    nodes: &'a [Node],
    existing: &'a [ShardAssignment],
    caught_up: &'a dyn CaughtUp,
) -> Catalog<'a> {
    Catalog {
        streams,
        caches: &[],
        nodes,
        existing,
        caught_up,
        policy: MovePolicy::default(),
    }
}

fn orders() -> ShardKey {
    pinned("orders", 0, "any").key
}

/// The case the control exists for: an unreplicated durable shard whose
/// broker is down. The shard is placed afresh on a live node, over the
/// generation it was decided from.
#[test]
fn an_unreplicated_shard_whose_owner_is_down_can_be_abandoned() {
    let streams = vec![stream("orders", 1)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Down, None),
        node("broker-b", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &[])];

    let decided = abandon_log(
        &catalog(&streams, &nodes, &existing, &NothingCaughtUp),
        &orders(),
    )
    .expect("abandoned");

    assert_eq!(
        decided.step,
        MoveStep::Discard {
            from: "broker-a".to_string(),
            to: "broker-b".to_string(),
        }
    );
    assert_eq!(decided.assignment.leader, "broker-b");
    assert_eq!(decided.assignment.state, ShardState::Assigning);
    assert_eq!(decided.expected_generation, existing[0].generation);
}

/// A replicated shard with no replica holding the log is stranded the same
/// way, and the replica set is rebuilt around the new leader.
#[test]
fn a_replicated_shard_with_no_caught_up_replica_can_be_abandoned() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b"])];

    let decided = abandon_log(
        &catalog(&streams, &nodes, &existing, &NothingCaughtUp),
        &orders(),
    )
    .expect("abandoned");

    assert_eq!(decided.assignment.replicas.len(), 1);
    assert!(
        !decided
            .assignment
            .replicas
            .contains(&decided.assignment.leader)
    );
}

/// Refused wherever the log is not lost: a leader that is serving, a
/// draining one still handing it off, or a replica that can take over.
#[test]
fn a_shard_that_is_not_stranded_is_refused() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let refused = |leader: NodeLifecycle, caught_up: &[&str]| {
        let nodes = vec![
            node("broker-a", leader, None),
            node("broker-b", NodeLifecycle::Live, None),
        ];
        let existing = vec![assigned("orders", "broker-a", &["broker-b"])];
        let caught_up = CaughtUpNodes(caught_up.iter().map(|n| n.to_string()).collect());
        abandon_log(&catalog(&streams, &nodes, &existing, &caught_up), &orders())
    };

    assert_eq!(
        refused(NodeLifecycle::Live, &[]).unwrap_err(),
        Refused::NotStranded
    );
    assert_eq!(
        refused(NodeLifecycle::Draining, &[]).unwrap_err(),
        Refused::NotStranded
    );
    assert_eq!(
        refused(NodeLifecycle::Down, &["broker-b"]).unwrap_err(),
        Refused::NotStranded,
        "a caught-up replica fails over without loss",
    );
}

/// An ephemeral shard is never stranded: placement moves it on its own.
#[test]
fn an_ephemeral_shard_is_refused() {
    let streams = vec![ephemeral_stream("orders", 1)];
    let nodes = vec![node("broker-b", NodeLifecycle::Live, None)];
    let existing = vec![assigned("orders", "broker-a", &[])];

    assert_eq!(
        abandon_log(
            &catalog(&streams, &nodes, &existing, &NothingCaughtUp),
            &orders()
        ),
        Err(Refused::NotStranded)
    );
}

/// Nothing is given up when nothing could take the shard.
#[test]
fn with_no_live_node_nothing_is_abandoned() {
    let streams = vec![stream("orders", 1)];
    let nodes = vec![node("broker-a", NodeLifecycle::Down, None)];
    let existing = vec![assigned("orders", "broker-a", &[])];

    assert_eq!(
        abandon_log(
            &catalog(&streams, &nodes, &existing, &NothingCaughtUp),
            &orders()
        ),
        Err(Refused::Unplaceable(Unplaceable::NoEligibleNode))
    );
}

#[test]
fn a_shard_with_no_assignment_is_unknown() {
    let streams = vec![stream("orders", 1)];
    let nodes = vec![node("broker-b", NodeLifecycle::Live, None)];

    assert_eq!(
        abandon_log(&catalog(&streams, &nodes, &[], &NothingCaughtUp), &orders()),
        Err(Refused::UnknownShard)
    );
}

/// Through the store: the abandoned shard is written at a new generation on a
/// live node, and placement then keeps it there.
#[tokio::test]
async fn an_abandoned_shard_is_written_and_kept() {
    use super::reconciler::{cluster, shard_zero};
    let store = cluster(&["broker-a", "broker-b"]).await;
    let liveness = Default::default();
    reconcile_once(&store, &liveness, MovePolicy::default()).await;
    let before = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    store
        .set_node_lifecycle(&before.leader, NodeLifecycle::Down)
        .await
        .expect("down");

    let (step, written) = run_operator(
        &store,
        &liveness,
        MovePolicy::default(),
        &PlacementWakes::default(),
        |catalog| abandon_log(catalog, &shard_zero()),
    )
    .await
    .expect("abandoned");

    assert!(matches!(step, MoveStep::Discard { .. }), "{step:?}");
    assert_ne!(written.leader, before.leader);
    assert!(written.generation > before.generation);
    let outcome = reconcile_once(&store, &liveness, MovePolicy::default()).await;
    assert_eq!(
        store
            .get_shard_assignment(&shard_zero())
            .await
            .expect("get"),
        written,
        "placement undid the abandonment: {outcome:?}",
    );
}
