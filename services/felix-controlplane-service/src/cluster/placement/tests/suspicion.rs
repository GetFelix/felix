//! A leader a majority of its set cannot reach is replaced while it still
//! heartbeats, on a shard whose deposed leader the fence keeps out.
use super::*;

/// Who holds the log, and which followers cannot reach which leaders.
struct Said {
    caught_up: BTreeSet<String>,
    cannot_reach: Vec<(&'static str, &'static str)>,
}

impl CaughtUp for Said {
    fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
        self.caught_up.contains(node_id)
    }

    fn suspects(&self, _key: &ShardKey, follower: &str, leader: &str) -> bool {
        self.cannot_reach.contains(&(follower, leader))
    }
}

fn quorum_stream() -> Stream {
    Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 3)
    }
}

fn said(caught_up: &[&str], cannot_reach: &[(&'static str, &'static str)]) -> Said {
    Said {
        caught_up: caught_up.iter().map(|id| id.to_string()).collect(),
        cannot_reach: cannot_reach.to_vec(),
    }
}

fn decision(streams: &[Stream], existing: &[ShardAssignment], said: &Said) -> Decision {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let plan = plan(streams, &[], &nodes, existing, said);
    plan.shards.into_iter().next().expect("one shard").decision
}

/// **Both followers cannot reach the leader: a follower that holds the log
/// takes over, and the old leader stays in the set.** It still heartbeats, so
/// the lease would never have moved the shard.
#[test]
fn a_leader_a_majority_cannot_reach_is_replaced_while_it_heartbeats() {
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let said = said(
        &["broker-b"],
        &[("broker-b", "broker-a"), ("broker-c", "broker-a")],
    );
    match decision(&[quorum_stream()], &existing, &said) {
        Decision::Place(leader, replicas) => {
            assert_eq!(leader, "broker-b");
            let mut replicas = replicas;
            replicas.sort();
            assert_eq!(replicas, vec!["broker-a", "broker-c"]);
        }
        other => panic!("expected a promotion, got {other:?}"),
    }
}

/// **One follower is not a majority.** It may be the one cut off; the
/// leader and the other follower can still acknowledge.
#[test]
fn one_follower_alone_does_not_move_the_shard() {
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let said = said(&["broker-b"], &[("broker-b", "broker-a")]);
    assert!(
        !matches!(
            decision(&[quorum_stream()], &existing, &said),
            Decision::Place(..)
        ),
        "a single follower's word replaced a leader a majority can still reach",
    );
}

/// **A `Leader` stream waits for the lease.** Its writes are acknowledged by
/// the leader alone, so only the lease keeps a deposed one out.
#[test]
fn a_leader_stream_is_not_moved_on_the_followers_word() {
    let leader_stream = replicated_stream("orders", 1, 3);
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let said = said(
        &["broker-b"],
        &[("broker-b", "broker-a"), ("broker-c", "broker-a")],
    );
    assert!(!matches!(
        decision(&[leader_stream], &existing, &said),
        Decision::Place(..)
    ));
}

/// **With no follower to promote, the leader keeps the shard.** It is not
/// left unplaced, as a lost leader's shard would be: the leader is alive,
/// and may be the one that can still be reached.
#[test]
fn with_nobody_caught_up_the_leader_keeps_the_shard() {
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let said = said(&[], &[("broker-b", "broker-a"), ("broker-c", "broker-a")]);
    let decision = decision(&[quorum_stream()], &existing, &said);
    assert!(
        !matches!(decision, Decision::Place(..) | Decision::Unplaceable(_)),
        "{decision:?}"
    );
}

/// **Not in the middle of a move.** The move changes the set the next fence
/// counts, and ends through its own steps.
#[test]
fn a_shard_that_is_moving_is_left_to_the_move() {
    let mut moving = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    moving.successor = Some("broker-c".to_string());
    moving.move_started_at_millis = Some(1);
    moving.move_reason = Some(crate::model::MoveReason::Balance);
    let said = said(
        &["broker-b"],
        &[("broker-b", "broker-a"), ("broker-c", "broker-a")],
    );
    assert!(!matches!(
        decision(&[quorum_stream()], &[moving], &said),
        Decision::Place(..)
    ));
}
