//! A Raft set gains a member in four ways: the first full placement
//! (`raft_fill`), a top-up of a short set, the swap that replaces a lost
//! voter, and the same swap after a re-form, at a later epoch. Each one
//! seats only a broker the hybrid would have given a copy: never an
//! internal node, in the zone the set lacks first, and only inside the
//! stream's region or one its bridges lead to (#2992).
use std::sync::Arc;

use felix_common::membership::{NODE_ROLE_INTERNAL, NODE_ROLE_LABEL};
use felix_router::RegionRouter;

use super::restore::{MINUTE, Reports, gone};
use super::*;
use crate::model::{PlacedInEpoch, RaftFormed, RaftTarget, RaftVoters};

fn raft() -> MovePolicy {
    MovePolicy {
        raft_streams: true,
        ..MovePolicy::default()
    }
}

fn bridged(pairs: &[(&str, &str)]) -> MovePolicy {
    MovePolicy {
        regions: Arc::new(RegionRouter::with_bridges(
            String::new(),
            pairs.iter().map(|(a, b)| (a.to_string(), b.to_string())),
        )),
        ..raft()
    }
}

fn internal(id: &str) -> Node {
    let mut node = node(id, NodeLifecycle::Live, None);
    node.spec
        .labels
        .insert(NODE_ROLE_LABEL.to_string(), NODE_ROLE_INTERNAL.to_string());
    node
}

fn zoned(mut node: Node, zone: &str) -> Node {
    node.spec.zone = Some(zone.to_string());
    node
}

fn in_region(mut node: Node, region: &str) -> Node {
    node.spec.region = region.to_string();
    node
}

fn up(id: &str) -> Node {
    node(id, NodeLifecycle::Live, None)
}

/// Gone past the replace delay, so its seat is given away.
fn lost(id: &str) -> Node {
    gone(id, 10 * MINUTE)
}

/// `orders` shard 0 on `leader` and `followers`, its group formed at
/// `epoch` with every member voting. Epoch 1 is the group a re-form
/// started: the set still names the brokers it was placed on.
fn formed(leader: &str, followers: &[&str], epoch: u64) -> ShardAssignment {
    let mut assignment = pinned("orders", 0, leader);
    assignment.replicas = followers.iter().map(|node| node.to_string()).collect();
    let members: BTreeMap<String, u64> = assignment.nodes().map(|n| (n.clone(), 7)).collect();
    if epoch > 0 {
        assignment.placed = Some(PlacedInEpoch {
            epoch,
            brokers: members.keys().cloned().collect(),
        });
    }
    assignment.raft_target = Some(RaftTarget {
        counter: 2,
        members: members.clone(),
    });
    assignment.raft_formed = Some(RaftFormed {
        incarnation: assignment.incarnation,
        epoch,
    });
    assignment.raft_voters = Some(RaftVoters {
        incarnation: assignment.incarnation,
        epoch,
        index: 1,
        members: members.clone(),
    });
    assignment.raft_copy_ids = members;
    assignment
}

/// `orders` shard 0 placed on too few brokers for its group to form.
fn thin(leader: &str) -> ShardAssignment {
    let mut assignment = pinned("orders", 0, leader);
    assignment.placed = Some(PlacedInEpoch {
        epoch: 0,
        brokers: [leader.to_string()].into(),
    });
    assignment
}

fn homed(region: &str) -> Stream {
    Stream {
        region: Some(region.to_string()),
        ..replicated_stream("orders", 1, 3)
    }
}

fn pass(
    streams: &[Stream],
    nodes: &[Node],
    existing: &[ShardAssignment],
    policy: MovePolicy,
) -> Plan {
    plan_with(streams, &[], nodes, existing, &Reports::default(), policy)
}

/// Every broker the plan's writes add to `orders` shard 0, by any step.
fn seated(plan: &Plan, before: &ShardAssignment) -> BTreeSet<String> {
    plan.moves()
        .filter(|(key, _, _)| **key == before.key)
        .flat_map(|(_, _, written)| written.nodes().cloned().collect::<Vec<_>>())
        .filter(|node| !before.nodes().any(|held| held == node))
        .collect()
}

/// The one swap the plan makes, as (from, to).
fn the_swap(plan: &Plan) -> (String, String) {
    let moves: Vec<_> = plan.moves().collect();
    assert_eq!(moves.len(), 1, "{plan:?}");
    match moves[0].1 {
        MoveStep::RaftReplace { from, to } => (from.clone(), to.clone()),
        other => panic!("expected a swap, got {other:?}"),
    }
}

fn the_top_up(plan: &Plan) -> String {
    let moves: Vec<_> = plan.moves().collect();
    assert_eq!(moves.len(), 1, "{plan:?}");
    match moves[0].1 {
        MoveStep::RaftTopUp { to } => to.clone(),
        other => panic!("expected a top-up, got {other:?}"),
    }
}

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

/// **A lost voter is never replaced by an internal node**, though it is the
/// only spare and holds nothing. The shard waits, at epoch 0 and in a
/// re-formed group, until an ordinary broker joins and takes the seat.
#[test]
fn a_lost_voter_waits_rather_than_seat_an_internal_node() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    for epoch in [0, 1] {
        let existing = formed("broker-a", &["broker-b", "broker-c"], epoch);
        let mut nodes = vec![up("broker-a"), up("broker-b"), lost("broker-c")];
        nodes.push(internal("core"));

        let held = pass(&streams, &nodes, std::slice::from_ref(&existing), raft());
        assert_eq!(held.moves().count(), 0, "epoch {epoch}: {held:?}");
        assert!(
            held.waiting()
                .any(|(_, why)| matches!(why, Blocked::NoReplacement { .. })),
            "epoch {epoch}: {held:?}"
        );

        nodes.push(up("broker-d"));
        let plan = pass(&streams, &nodes, std::slice::from_ref(&existing), raft());
        assert_eq!(
            the_swap(&plan),
            ("broker-c".to_string(), "broker-d".to_string()),
            "epoch {epoch}"
        );
    }
}

/// **A short set is never topped up onto an internal node**, as a re-form
/// or an abandon leaves one.
#[test]
fn a_short_set_is_never_topped_up_onto_an_internal_node() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    for epoch in [0, 1] {
        let existing = formed("broker-a", &["broker-b"], epoch);
        let mut nodes = vec![up("broker-a"), up("broker-b"), internal("core")];

        let held = pass(&streams, &nodes, std::slice::from_ref(&existing), raft());
        assert!(
            seated(&held, &existing).is_empty(),
            "epoch {epoch}: {held:?}"
        );

        nodes.push(up("broker-d"));
        let plan = pass(&streams, &nodes, std::slice::from_ref(&existing), raft());
        assert_eq!(the_top_up(&plan), "broker-d", "epoch {epoch}");
    }
}

/// **A thin set is never filled with an internal node.** With only `core`
/// to make up the factor the shard waits; once two ordinary brokers are
/// live it is filled with them.
#[test]
fn a_thin_set_is_never_filled_with_an_internal_node() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = thin("broker-a");
    let mut nodes = vec![up("broker-a"), up("broker-b"), internal("core")];

    let held = pass(&streams, &nodes, std::slice::from_ref(&existing), raft());
    assert!(seated(&held, &existing).is_empty(), "{held:?}");

    nodes.push(up("broker-c"));
    let plan = pass(&streams, &nodes, std::slice::from_ref(&existing), raft());
    assert_eq!(
        seated(&plan, &existing),
        set(&["broker-b", "broker-c"]),
        "{plan:?}"
    );
}

/// **A lost voter is replaced in the zone it held**, so the set still spans
/// every zone and survives losing any one. `broker-d` holds fewer shards
/// than `broker-e`, so a zone-blind choice takes it and leaves two copies
/// in `z1`. The internal `core`, in `z3` and holding nothing, is never
/// taken either.
#[test]
fn a_lost_voter_is_replaced_in_the_zone_it_held() {
    let streams = vec![replicated_stream("orders", 1, 3), stream("other", 2)];
    for epoch in [0, 1] {
        let existing = vec![
            formed("broker-a", &["broker-b", "broker-c"], epoch),
            pinned("other", 0, "broker-e"),
            pinned("other", 1, "broker-e"),
        ];
        let nodes = vec![
            zoned(up("broker-a"), "z1"),
            zoned(up("broker-b"), "z2"),
            zoned(lost("broker-c"), "z3"),
            zoned(up("broker-d"), "z1"),
            zoned(up("broker-e"), "z3"),
            zoned(internal("core"), "z3"),
        ];

        let plan = pass(&streams, &nodes, &existing, raft());

        let swaps: Vec<_> = plan
            .moves()
            .filter(|(key, _, _)| **key == existing[0].key)
            .collect();
        assert_eq!(swaps.len(), 1, "epoch {epoch}: {plan:?}");
        assert!(
            matches!(swaps[0].1, MoveStep::RaftReplace { from, to }
                if from == "broker-c" && to == "broker-e"),
            "epoch {epoch}: {plan:?}"
        );
    }
}

/// **A lost voter of a homed stream is replaced only where the stream may
/// be.** `eu>us` lets an `eu` stream into `us`; `ap>eu` does not let it
/// into `ap`. Without the `eu>us` bridge the shard waits.
#[test]
fn a_lost_voter_is_replaced_only_in_a_region_the_stream_may_be_in() {
    let streams = vec![homed("eu")];
    for epoch in [0, 1] {
        let existing = formed("eu-1", &["eu-2", "eu-3"], epoch);
        let nodes = vec![
            in_region(up("eu-1"), "eu"),
            in_region(up("eu-2"), "eu"),
            in_region(lost("eu-3"), "eu"),
            in_region(up("ap-1"), "ap"),
            in_region(up("us-1"), "us"),
        ];
        let existing = std::slice::from_ref(&existing);

        let plan = pass(
            &streams,
            &nodes,
            existing,
            bridged(&[("eu", "us"), ("ap", "eu")]),
        );
        assert_eq!(
            the_swap(&plan),
            ("eu-3".to_string(), "us-1".to_string()),
            "epoch {epoch}"
        );

        let held = pass(&streams, &nodes, existing, bridged(&[("ap", "eu")]));
        assert_eq!(held.moves().count(), 0, "epoch {epoch}: {held:?}");
        assert!(
            held.waiting()
                .any(|(_, why)| matches!(why, Blocked::NoReplacement { .. })),
            "epoch {epoch}: {held:?}"
        );
    }
}

/// The same rule for a short set's top-up and a thin set's fill.
#[test]
fn a_homed_set_grows_only_into_a_region_the_stream_may_be_in() {
    let streams = vec![homed("eu")];
    let nodes = vec![
        in_region(up("eu-1"), "eu"),
        in_region(up("eu-2"), "eu"),
        in_region(up("ap-1"), "ap"),
        in_region(up("us-1"), "us"),
    ];
    let open = bridged(&[("eu", "us"), ("ap", "eu")]);
    let closed = bridged(&[("ap", "eu")]);

    let short = formed("eu-1", &["eu-2"], 1);
    let short = std::slice::from_ref(&short);
    assert_eq!(
        the_top_up(&pass(&streams, &nodes, short, open.clone())),
        "us-1"
    );
    assert_eq!(
        pass(&streams, &nodes, short, closed.clone())
            .moves()
            .count(),
        0
    );

    let thin = thin("eu-1");
    let plan = pass(&streams, &nodes, std::slice::from_ref(&thin), open);
    assert_eq!(seated(&plan, &thin), set(&["eu-2", "us-1"]), "{plan:?}");
    let held = pass(&streams, &nodes, std::slice::from_ref(&thin), closed);
    assert!(seated(&held, &thin).is_empty(), "{held:?}");
}
