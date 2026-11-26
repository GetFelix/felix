//! A node whose broker registered without durable storage holds only
//! in-memory stream shards: no durable stream's or cache's shard is placed,
//! restored or moved there, and one already there is moved off.
use felix_common::membership::{NODE_STORAGE_LABEL, NODE_STORAGE_NONE};

use super::caches::cache;
use super::*;

fn diskless(id: &str) -> Node {
    let mut node = node(id, NodeLifecycle::Live, None);
    node.spec.labels.insert(
        NODE_STORAGE_LABEL.to_string(),
        NODE_STORAGE_NONE.to_string(),
    );
    node
}

fn in_memory(name: &str, shards: u32) -> Stream {
    Stream {
        durable: false,
        ..stream(name, shards)
    }
}

/// Both engines.
fn policies() -> [MovePolicy; 2] {
    [
        MovePolicy::default(),
        MovePolicy {
            raft_streams: true,
            ..MovePolicy::default()
        },
    ]
}

fn holders(plan: &Plan, name: &str) -> BTreeSet<String> {
    plan.to_place()
        .filter(|(key, _, _)| key.stream == name)
        .flat_map(|(_, leader, replicas)| {
            std::iter::once(leader.to_string()).chain(replicas.iter().cloned())
        })
        .collect()
}

#[test]
fn a_diskless_node_is_given_in_memory_shards_only() {
    let mut nodes = live(&["a", "b", "c"]);
    nodes.push(diskless("bare"));
    let streams = vec![
        replicated_stream("orders", 12, 3),
        stream("audit", 12),
        in_memory("ticks", 12),
    ];
    let caches = vec![cache("sessions", 12)];
    for policy in policies() {
        let plan = plan_with(&streams, &caches, &nodes, &[], &NothingCaughtUp, policy);

        assert_eq!(plan.to_place().count(), 48);
        for durable in ["orders", "audit", "sessions"] {
            let held = holders(&plan, durable);
            assert!(!held.contains("bare"), "{durable} placed on bare: {held:?}");
        }
        assert!(holders(&plan, "ticks").contains("bare"), "{plan:?}");
    }
}

#[test]
fn a_cluster_of_diskless_nodes_places_only_in_memory_shards() {
    let nodes = vec![diskless("bare-1"), diskless("bare-2")];
    let streams = vec![stream("orders", 2), in_memory("ticks", 2)];
    let caches = vec![cache("sessions", 2)];
    for policy in policies() {
        let plan = plan_with(&streams, &caches, &nodes, &[], &NothingCaughtUp, policy);

        assert!(holders(&plan, "orders").is_empty(), "{plan:?}");
        assert!(holders(&plan, "sessions").is_empty(), "{plan:?}");
        assert_eq!(holders(&plan, "ticks").len(), 2, "{plan:?}");
    }
}

/// Brokers `a`, `b` and `bare` hold a Raft stream's three copies; `bare`
/// came back without its disk after the group formed.
fn raft_member_on_diskless() -> Vec<ShardAssignment> {
    let mut assignment = assigned("orders", "a", &["b", "bare"]);
    let members: BTreeMap<String, u64> = ["a", "b", "bare"]
        .iter()
        .map(|node| (node.to_string(), 7))
        .collect();
    assignment.raft_target = Some(crate::model::RaftTarget {
        counter: 2,
        members: members.clone().into_iter().collect(),
    });
    assignment.raft_formed = Some(crate::model::RaftFormed {
        incarnation: assignment.incarnation,
        epoch: 0,
    });
    assignment.raft_voters = Some(crate::model::RaftVoters {
        incarnation: assignment.incarnation,
        epoch: 0,
        index: 1,
        members: members.clone().into_iter().collect(),
    });
    assignment.raft_copy_ids = members.into_iter().collect();
    vec![assignment]
}

#[test]
fn a_raft_member_on_a_diskless_node_is_replaced() {
    let mut nodes = live(&["a", "b", "c"]);
    nodes.push(diskless("bare"));
    let streams = vec![replicated_stream("orders", 1, 3)];
    let raft = MovePolicy {
        raft_streams: true,
        ..MovePolicy::default()
    };

    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &raft_member_on_diskless(),
        &NothingCaughtUp,
        raft,
    );

    let (_, step, written) = plan.moves().next().expect("a set change");
    assert!(
        matches!(step, MoveStep::RaftReplace { from, to } if from == "bare" && to == "c"),
        "{step:?}"
    );
    assert!(!written.nodes().any(|n| n == "bare"), "{written:?}");
}

#[test]
fn an_operator_moves_only_in_memory_shards_onto_a_diskless_node() {
    let nodes = vec![
        node("a", NodeLifecycle::Live, None),
        node("b", NodeLifecycle::Live, None),
        diskless("bare"),
    ];
    let streams = vec![stream("orders", 1), in_memory("ticks", 1)];
    let caches = vec![cache("sessions", 1)];
    let mut on_a = assigned("sessions", "a", &[]);
    on_a.key.kind = ShardKind::Cache;
    let existing = vec![
        assigned("orders", "a", &[]),
        assigned("ticks", "a", &[]),
        on_a,
    ];
    let catalog = Catalog {
        streams: &streams,
        caches: &caches,
        nodes: &nodes,
        existing: &existing,
        caught_up: &NothingCaughtUp,
        policy: MovePolicy::default(),
    };

    for durable in [&existing[0], &existing[2]] {
        assert_eq!(
            start_move(&catalog, &durable.key, "bare"),
            Err(Refused::NoDurableStorage("bare".to_string()))
        );
        assert!(start_move(&catalog, &durable.key, "b").is_ok());
    }
    assert!(start_move(&catalog, &existing[1].key, "bare").is_ok());
}

/// A copy on a diskless node, left by an older control plane, is not one
/// the shard's spread can count on: an operator move's zone report leaves it
/// out.
#[test]
fn an_operator_moves_zone_report_leaves_out_a_diskless_node() {
    let zoned = |id: &str| {
        let mut node = if id.starts_with('d') {
            diskless(id)
        } else {
            node(id, NodeLifecycle::Live, None)
        };
        node.spec.zone = Some(id[..1].to_string());
        node
    };
    let nodes: Vec<Node> = ["a1", "b1", "c1", "d1"]
        .iter()
        .map(|id| zoned(id))
        .collect();
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = vec![assigned("orders", "a1", &["b1", "d1"])];
    let catalog = Catalog {
        streams: &streams,
        caches: &[],
        nodes: &nodes,
        existing: &existing,
        caught_up: &NothingCaughtUp,
        policy: MovePolicy::default(),
    };

    let decided = start_move(&catalog, &existing[0].key, "c1").expect("started");
    let zones = decided.zones.expect("brokers report zones");
    assert_eq!(zones.before, 2, "{zones:?}");
}

/// A durable shard with nowhere to go but diskless nodes says that is why,
/// with or without a home region.
#[test]
fn a_durable_shard_on_only_diskless_nodes_reports_the_storage() {
    let mut nodes = vec![diskless("bare")];
    nodes.push(node("away", NodeLifecycle::Live, None));
    nodes[1].spec.region = "eu-west-1".to_string();
    let homed = Stream {
        region: Some("us-west-2".to_string()),
        ..stream("orders", 1)
    };
    for streams in [vec![homed], vec![stream("orders", 1)]] {
        let only_bare = if streams[0].region.is_some() {
            nodes.clone()
        } else {
            vec![diskless("bare")]
        };
        let plan = plan(&streams, &[], &only_bare, &[], &NothingCaughtUp);
        assert_eq!(
            plan.shards[0].decision,
            Decision::Unplaceable(Unplaceable::NoDurableStorage),
            "{streams:?}"
        );
    }
}
