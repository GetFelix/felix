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

    fn suspects(&self, _key: &ShardKey, _generation: u64, follower: &str, leader: &str) -> bool {
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

/// Followers that have suspected the leader since `since`, at `now`.
struct HeldSince {
    said: Said,
    since: Option<u64>,
    now: u64,
}

impl CaughtUp for HeldSince {
    fn is_caught_up(&self, key: &ShardKey, node_id: &str) -> bool {
        self.said.is_caught_up(key, node_id)
    }

    fn suspects(&self, key: &ShardKey, generation: u64, follower: &str, leader: &str) -> bool {
        self.said.suspects(key, generation, follower, leader)
    }

    fn suspected_since_millis(&self, _follower: &str, _leader: &str) -> Option<u64> {
        self.since
    }

    fn as_of_millis(&self) -> Option<u64> {
        Some(self.now)
    }
}

/// **Under `replica_election` the followers' word waits 2×S**, as this
/// instance watched it, so placement does not race the replicas' own
/// election; it is the fallback. Without the feature it acts at once.
#[test]
fn under_replica_election_the_followers_word_is_held_for_twice_s() {
    let hold = MovePolicy::default()
        .election_hold_millis
        .expect("a default");
    assert_eq!(hold, 10_000, "2×S at the brokers' default S of 5 s");
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let held = |since: Option<u64>| HeldSince {
        said: said(
            &["broker-b"],
            &[("broker-b", "broker-a"), ("broker-c", "broker-a")],
        ),
        since,
        now: 100_000,
    };
    let electing = MovePolicy {
        replica_election: true,
        ..MovePolicy::default()
    };
    let decide = |caught_up: &HeldSince, policy: MovePolicy| {
        plan_with(
            &[quorum_stream()],
            &[],
            &nodes,
            &existing,
            caught_up,
            policy,
        )
        .shards
        .remove(0)
        .decision
    };
    let promoted = |decision: &Decision| matches!(decision, Decision::Place(leader, _) if leader == "broker-b");

    let young = held(Some(100_000 - hold + 1));
    assert!(
        !promoted(&decide(&young, electing.clone())),
        "promoted inside 2×S"
    );
    // Never watched by this instance: no evidence it has lasted.
    assert!(!promoted(&decide(&held(None), electing.clone())));
    assert!(promoted(&decide(
        &held(Some(100_000 - hold)),
        electing.clone()
    )));
    // Without the feature, or with the hold off, nothing waits.
    assert!(promoted(&decide(&young, MovePolicy::default())));
    assert!(promoted(&decide(
        &young,
        MovePolicy {
            election_hold_millis: None,
            ..electing
        }
    )));
}

/// An instance dates a suspicion from when it first saw it, and starts
/// again once it lapses.
#[test]
fn an_instance_dates_a_suspicion_from_when_it_first_saw_it() {
    use super::super::progress::CopyProgress;
    use crate::model::NodeSuspicion;
    let liveness = crate::config::NodeLivenessConfig::default();
    let at = |now: u64, suspects: &[&str]| {
        let suspicions = if suspects.is_empty() {
            Vec::new()
        } else {
            vec![NodeSuspicion {
                node_id: "broker-b".to_string(),
                incarnation: 1,
                suspects: suspects.iter().map(|s| s.to_string()).collect(),
                reported_at_millis: now,
            }]
        };
        crate::cluster::placement::ReplicaPositions::new(Vec::new(), &liveness, now)
            .with_suspicions(
                suspicions,
                Default::default(),
                &liveness,
                &Default::default(),
            )
    };
    let id = ("broker-b".to_string(), "broker-a".to_string());
    let mut progress = CopyProgress::default();
    assert_eq!(
        progress
            .observe_suspicions(&at(100, &["broker-a"]))
            .get(&id),
        Some(&100)
    );
    assert_eq!(
        progress
            .observe_suspicions(&at(900, &["broker-a"]))
            .get(&id),
        Some(&100)
    );
    assert!(progress.observe_suspicions(&at(950, &[])).is_empty());
    assert_eq!(
        progress
            .observe_suspicions(&at(990, &["broker-a"]))
            .get(&id),
        Some(&990)
    );
}
