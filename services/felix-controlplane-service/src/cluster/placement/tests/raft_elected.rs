//! A Raft shard whose named leader is down: placement names the member the
//! group reported electing, since the group's leader does not hand over
//! while a voter is down (#3041).
use super::*;
use crate::model::{RaftLeader, RaftTarget, RaftVoters};

const ABC: [&str; 3] = ["broker-a", "broker-b", "broker-c"];

fn raft() -> MovePolicy {
    MovePolicy {
        raft_streams: true,
        ..MovePolicy::default()
    }
}

/// Shard 0 of `orders` led by `leader`, every other broker of `set` a
/// member, the group formed on the whole set.
fn formed(leader: &str, set: &[&str]) -> ShardAssignment {
    let mut a = pinned("orders", 0, leader);
    a.replicas = set
        .iter()
        .filter(|node| **node != leader)
        .map(|node| node.to_string())
        .collect();
    let copies: BTreeMap<String, u64> = set.iter().map(|node| (node.to_string(), 7)).collect();
    a.raft_target = Some(RaftTarget {
        counter: 2,
        members: copies.clone(),
    });
    a.raft_formed = Some(crate::model::RaftFormed {
        incarnation: a.incarnation,
        epoch: 0,
    });
    a.raft_voters = Some(RaftVoters {
        incarnation: a.incarnation,
        epoch: 0,
        index: 1,
        members: copies.clone(),
    });
    a.raft_copy_ids = copies;
    a
}

fn reported(mut a: ShardAssignment, node: &str, term: u64) -> ShardAssignment {
    a.raft_leader = Some(RaftLeader {
        incarnation: a.incarnation,
        epoch: 0,
        term,
        node: node.to_string(),
    });
    a
}

fn down(mut nodes: Vec<Node>, ids: &[&str]) -> Vec<Node> {
    for node in &mut nodes {
        if ids.contains(&node.node_id.as_str()) {
            node.status.lifecycle = NodeLifecycle::Down;
        }
    }
    nodes
}

/// The leader one pass names, or `None` when it writes nothing.
fn named(existing: &ShardAssignment, nodes: &[Node]) -> Option<String> {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let plan = plan_with(
        &streams,
        &[],
        nodes,
        std::slice::from_ref(existing),
        &NothingCaughtUp,
        raft(),
    );
    plan.moves()
        .map(|(_, _, written)| written.leader.clone())
        .next()
}

/// What placement names with no report: the member leading fewest.
fn fewest_choice(nodes: &[Node]) -> String {
    named(&formed("broker-a", &ABC), nodes).expect("renamed")
}

/// The live member placement would not pick on its own.
fn other_than(choice: &str) -> &'static str {
    if choice == "broker-b" {
        "broker-c"
    } else {
        "broker-b"
    }
}

/// `count` passes, each one's write fed back as the next one's record.
/// Returns the record after the last pass, every leader a write changed to,
/// and the last pass's decision.
fn passes(
    mut existing: ShardAssignment,
    nodes: &[Node],
    streams: &[Stream],
    policy: &MovePolicy,
    count: usize,
) -> (ShardAssignment, Vec<String>, Decision) {
    let mut names = Vec::new();
    let mut last = Decision::Kept;
    for _ in 0..count {
        let plan = plan_with(
            streams,
            &[],
            nodes,
            std::slice::from_ref(&existing),
            &NothingCaughtUp,
            policy.clone(),
        );
        last = plan.shards[0].decision.clone();
        if let Decision::Move(_, written) = &last {
            if written.leader != existing.leader {
                names.push(written.leader.clone());
            }
            existing = ShardAssignment {
                raft_leader: existing.raft_leader.clone(),
                ..written.clone()
            };
        }
    }
    (existing, names, last)
}

/// [`passes`] over `orders` at RF 3 under [`raft`], until a pass writes
/// nothing; panics if five do not get there.
fn settle(existing: ShardAssignment, nodes: &[Node]) -> (ShardAssignment, Vec<String>) {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let (after, names, last) = passes(existing, nodes, &streams, &raft(), 5);
    assert!(
        !matches!(last, Decision::Move(..)),
        "placement never settled: {names:?}"
    );
    (after, names)
}

#[test]
fn a_down_leader_is_renamed_to_the_elected_member() {
    let nodes = down(live(&ABC), &["broker-a"]);
    let elected = other_than(&fewest_choice(&nodes));
    let existing = reported(formed("broker-a", &ABC), elected, 5);

    let (after, names) = settle(existing, &nodes);
    assert_eq!(
        names,
        vec![elected.to_string()],
        "one write, to the elected"
    );
    assert_eq!(after.leader, elected);
}

#[test]
fn a_report_after_the_rename_is_adopted_and_kept() {
    let nodes = down(live(&ABC), &["broker-a"]);
    let first = fewest_choice(&nodes);
    // No report yet: today's choice.
    let (renamed, names) = settle(formed("broker-a", &ABC), &nodes);
    assert_eq!(names, vec![first.clone()]);

    // The group elected the other one, which cannot hand over while a is
    // down: placement follows it, once.
    let elected = other_than(&first);
    let (after, names) = settle(reported(renamed, elected, 5), &nodes);
    assert_eq!(names, vec![elected.to_string()]);
    assert_eq!(after.leader, elected);

    // Once a is back the group can hand over, so a stale report no longer
    // renames anything and balancing decides as before.
    let back = live(&ABC);
    let (_, names) = settle(reported(after, &first, 5), &back);
    assert!(names.is_empty(), "{names:?}");
}

#[test]
fn the_higher_term_report_wins() {
    let nodes = down(live(&ABC), &["broker-a"]);
    let mut existing = formed("broker-a", &ABC);
    let floor = |term: u64| crate::model::RaftFloorReport {
        key: existing.key.clone(),
        incarnation: existing.incarnation,
        epoch: 0,
        index: term * 10,
        term,
        voters: None,
    };
    let (older, newer) = (floor(4), floor(5));
    let elected = other_than(&fewest_choice(&nodes));
    existing.note_raft_leader(other_than(elected), &older);
    existing.note_raft_leader(elected, &newer);
    // The deposed leader keeps reporting its term.
    existing.note_raft_leader(other_than(elected), &older);

    let (after, names) = settle(existing, &nodes);
    assert_eq!(names, vec![elected.to_string()]);
    assert_eq!(after.leader, elected);
}

#[test]
fn a_report_naming_a_non_voter_or_a_down_broker_is_ignored() {
    // Naming the down leader itself, a broker outside the set, or a member
    // the reported voters leave out: placement chooses as with no report.
    let nodes = down(live(&ABC), &["broker-a"]);
    let first = fewest_choice(&nodes);
    let other = other_than(&first);
    let mut not_voting = reported(formed("broker-a", &ABC), other, 5);
    not_voting
        .raft_voters
        .as_mut()
        .expect("voters")
        .members
        .remove(other);
    for existing in [
        reported(formed("broker-a", &ABC), "broker-a", 5),
        reported(formed("broker-a", &ABC), "broker-x", 5),
        not_voting,
    ] {
        assert_eq!(
            named(&existing, &nodes),
            Some(first.clone()),
            "{existing:?}"
        );
    }

    // A report naming a member that is down too.
    let abcd = ["broker-a", "broker-b", "broker-c", "broker-d"];
    let nodes = down(live(&abcd), &["broker-a", "broker-d"]);
    let existing = reported(formed("broker-a", &abcd), "broker-d", 5);
    let leader = named(&existing, &nodes).expect("renamed");
    assert_ne!(leader, "broker-d");
}

#[test]
fn repeated_passes_do_not_flap() {
    let nodes = down(live(&ABC), &["broker-a"]);
    let streams = vec![replicated_stream("orders", 1, 3)];
    let elected = other_than(&fewest_choice(&nodes));
    let mut existing = reported(formed("broker-a", &ABC), elected, 5);
    let mut changes = Vec::new();
    for round in 0..10 {
        let (after, names, _) = passes(existing, &nodes, &streams, &raft(), 1);
        changes.extend(names);
        existing = after;
        // Reports at the same term from either member, in either order,
        // change nothing.
        let order = if round % 2 == 0 {
            ["broker-b", "broker-c"]
        } else {
            ["broker-c", "broker-b"]
        };
        for node in order {
            existing.note_raft_leader(
                node,
                &crate::model::RaftFloorReport {
                    key: existing.key.clone(),
                    incarnation: existing.incarnation,
                    epoch: 0,
                    index: 60,
                    term: 5,
                    voters: None,
                },
            );
        }
    }
    assert_eq!(changes, vec![elected.to_string()]);
    assert_eq!(existing.leader, elected);
}

fn internal(mut node: Node) -> Node {
    use felix_common::membership::{NODE_ROLE_INTERNAL, NODE_ROLE_LABEL};
    node.spec
        .labels
        .insert(NODE_ROLE_LABEL.to_string(), NODE_ROLE_INTERNAL.to_string());
    node
}

/// The group elected a broker the leave rules rename a leader away from,
/// while another member is down, so the group cannot hand over. Placement
/// keeps its own choice instead of following the group: its leader changes
/// at most once over many passes, and never back to the leaving broker.
#[test]
fn a_leaving_elected_broker_is_not_followed() {
    let mut region = live(&ABC);
    for node in &mut region {
        node.spec.region = if node.node_id == "broker-a" {
            "us"
        } else {
            "eu"
        }
        .to_string();
    }
    let homed = vec![Stream {
        region: Some("eu".to_string()),
        ..replicated_stream("orders", 1, 3)
    }];
    let plain = vec![replicated_stream("orders", 1, 3)];
    let no_bridges = MovePolicy {
        regions: std::sync::Arc::new(felix_router::RegionRouter::with_bridges(
            String::new(),
            std::iter::empty(),
        )),
        ..raft()
    };
    let mut draining = live(&ABC);
    draining[0].status.lifecycle = NodeLifecycle::Draining;
    let mut internal_a = live(&ABC);
    internal_a[0] = internal(internal_a[0].clone());
    let cases = [
        ("draining", draining, &plain, raft()),
        ("internal", internal_a, &plain, raft()),
        ("out of region", region, &homed, no_bridges),
    ];
    for (why, nodes, streams, policy) in cases {
        let nodes = down(nodes, &["broker-c"]);
        // Placement names a, as the group elected; and placement names b
        // while the report says a.
        for leader in ["broker-a", "broker-b"] {
            let existing = reported(formed(leader, &ABC), "broker-a", 5);
            let (after, names, last) = passes(existing, &nodes, streams, &policy, 6);
            assert!(names.len() <= 1, "{why}, from {leader}: {names:?}");
            assert_ne!(after.leader, "broker-a", "{why}, from {leader}");
            assert!(!matches!(last, Decision::Move(..)), "{why}: {last:?}");
        }
    }
}

/// A follower on a draining broker is still swapped out when every member
/// serves; while another member is down the swap waits, and a report
/// naming the elected member is followed first.
#[test]
fn a_leaving_follower_is_swapped_unless_the_set_is_degraded() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let abcd = ["broker-a", "broker-b", "broker-c", "broker-d"];
    let mut nodes = live(&abcd);
    nodes[2].status.lifecycle = NodeLifecycle::Draining;
    let existing = reported(formed("broker-a", &ABC), "broker-a", 5);
    let (_, _, last) = passes(existing, &nodes, &streams, &raft(), 1);
    assert!(
        matches!(&last, Decision::Move(MoveStep::RaftReplace { from, .. }, _) if from == "broker-c"),
        "{last:?}"
    );

    let nodes = down(nodes, &["broker-b"]);
    let existing = reported(formed("broker-d", &abcd), "broker-a", 5);
    let (after, names, last) = passes(existing, &nodes, &streams, &raft(), 6);
    assert_eq!(names, vec!["broker-a".to_string()]);
    assert_eq!(after.nodes().count(), 4, "the set stays");
    assert!(matches!(last, Decision::Waiting(_)), "{last:?}");
}
