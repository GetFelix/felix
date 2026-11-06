//! The replica's set checks: `RefuseStaleSet`, `SetCurrent` and the set
//! half of `AnswerFence` in `docs/formal/FelixShard.tla`.

use std::time::Duration;

use felix_storage::disk_log::{Member, Membership, ReplicaSet};
use felix_wire::internal::{Fence, FenceElection, ReplicaLog, ReplicaSetView, SetMember};

use super::*;

fn view(generation: u64, voters: &[&str], learners: &[&str]) -> ReplicaSetView {
    let member = |node: &&str, learner| SetMember {
        node_id: (*node).to_string(),
        storage_id: None,
        learner,
    };
    ReplicaSetView {
        generation,
        members: voters
            .iter()
            .map(|node| member(node, false))
            .chain(learners.iter().map(|node| member(node, true)))
            .collect(),
    }
}

fn election_fence(generation: u64, set: ReplicaSetView, candidate: bool) -> Fence {
    Fence {
        correlation_id: 9,
        shard: batch(generation, 0, &[]).shard,
        log: ReplicaLog::Stream,
        election: Some(FenceElection {
            candidate,
            set,
            leaving: None,
            earlier: Vec::new(),
        }),
    }
}

fn voters(generation: u64) -> ReplicaSetView {
    view(generation, &["broker-a", LOCAL, "broker-c"], &[])
}

fn stamped(generation: u64, first_offset: u64, values: &[&str], set: u64) -> ReplicateRecords {
    ReplicateRecords {
        set_generation: Some(set),
        ..batch(generation, first_offset, values)
    }
}

async fn stream_log(broker: &Broker) -> felix_broker::StreamLog {
    broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
}

/// Seed this replica's log with a set at `generation`, as an earlier fence
/// would have.
async fn knows_set(broker: &Broker, generation: u64) {
    let log = stream_log(broker).await;
    log.accept_set(ReplicaSet {
        current: Membership {
            generation,
            members: vec![Member {
                node: LOCAL.to_string(),
                storage: None,
                learner: false,
                wiped: false,
            }],
        },
        leaving: None,
        earlier: Vec::new(),
    })
    .await
    .expect("set");
}

fn ok(answer: &InternalMessage) {
    assert!(
        matches!(answer, InternalMessage::FenceOk(_)),
        "expected the fence to be taken, got {answer:?}"
    );
}

/// A router at `generation` led by broker-a with `LOCAL` following, whose
/// watch saw `before` (leader first) as the set right before it, or saw
/// nothing before it when `before` is `None`.
pub(super) fn router_after(generation: u64, before: Option<&[&str]>) -> Arc<ShardRouter> {
    let router = router_with(&[LOCAL], generation);
    let nodes: HashMap<String, NodeRef> = ["broker-a", LOCAL, "broker-c"]
        .iter()
        .zip(7001..)
        .map(|(id, port)| (id.to_string(), node(id, port)))
        .collect();
    let priors = before.map(|before| {
        let prior = felix_router::PriorSet {
            generation,
            before: Some(Arc::new(felix_router::PlacedSet {
                leader: before[0].to_string(),
                replicas: before[1..].iter().map(|id| id.to_string()).collect(),
                successor: None,
                storage_ids: Default::default(),
                member_storage_ids: Default::default(),
            })),
        };
        (key(), prior)
    });
    let table = RoutingTable::build(
        [(
            key(),
            "broker-a".to_string(),
            vec![LOCAL.to_string()],
            generation,
        )],
        &nodes,
    )
    .with_priors(priors.into_iter().collect());
    router.publish(table, &nodes);
    router
}

/// A control plane whose confirmed read assigns `generation` to the sender,
/// which a copy with no promise needs under `joint_fence`.
struct AssignedToSender(u64);

#[async_trait::async_trait]
impl crate::confirmed::ConfirmedRoutes for AssignedToSender {
    async fn confirmed(&self, _key: &felix_router::ShardKey) -> crate::confirmed::Confirmed {
        crate::confirmed::Confirmed::Assigned {
            generation: self.0,
            leader: SENDER.expect("a sender").to_string(),
        }
    }
}

fn refused_for_set_before(answer: &InternalMessage) -> bool {
    matches!(
        answer,
        InternalMessage::ReplicateError(err)
            if err.code == ErrorCode::StaleRoute
                && err.detail.contains("does not know the set before")
    )
}

fn joint_marks() -> Arc<crate::quorum::QuorumMarks> {
    let fleet = felix_common::fleet::FleetGate::new([felix_common::fleet::JOINT_FENCE.name()]);
    fleet.restart(Some(felix_common::fleet::JOINT_FENCE.name()));
    Arc::new(crate::quorum::QuorumMarks::with_fleet(Arc::new(fleet)))
}

/// **Under `joint_fence` a batch teaches a set only with the control plane's
/// set before it**, kept as the one being left, as the leader keeps it. The
/// copy's own older set may be an elected leader's, so a follower that missed
/// the joint fence and took its own as the set being left could stand on a
/// set holding none of the leader's records. Without the set before, the
/// batch is refused until a fence carries it. `LearnsLeavingSet`.
#[tokio::test]
async fn a_batch_under_joint_fence_learns_the_set_being_left_only_from_the_control_plane() {
    let (broker, _dir) = broker_with_storage().await;
    knows_set(&broker, 3).await;
    let unknown = ReplicaHandler::new(Arc::clone(&broker), router_after(4, None))
        .with_fleet(joint_marks())
        .with_confirmed_routes(Arc::new(AssignedToSender(4)));
    let answer = settled(|| {
        unknown.apply(
            SENDER,
            stamped(4, 0, &["a"], 4),
            felix_broker::LogKind::Stream,
        )
    })
    .await;
    assert!(refused_for_set_before(&answer), "{answer:?}");
    assert_eq!(
        stream_log(&broker).await.set_generation(),
        3,
        "nothing is learned from a refused batch"
    );

    let watched = ReplicaHandler::new(
        Arc::clone(&broker),
        router_after(4, Some(&["broker-a", LOCAL, "broker-c"])),
    )
    .with_fleet(joint_marks())
    .with_confirmed_routes(Arc::new(AssignedToSender(4)));
    let answer = settled(|| {
        watched.apply(
            SENDER,
            stamped(4, 0, &["a"], 4),
            felix_broker::LogKind::Stream,
        )
    })
    .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let set = stream_log(&broker).await.replica_set().expect("a set");
    assert_eq!(set.current.generation, 4);
    let leaving = set
        .leaving
        .expect("the set before is kept as the one being left");
    assert_eq!(leaving.generation, 3);
    let nodes: Vec<&str> = leaving.members.iter().map(|m| m.node.as_str()).collect();
    assert_eq!(nodes, vec!["broker-a", LOCAL, "broker-c"]);
}

/// **Only a wiped learner learns a set from a batch without the set before
/// it.** A wiped voter votes and a staged learner may lead, so either could
/// stand on the new set without fencing the one being left; both are refused
/// until a fence carries it. `LearnsLeavingSet`.
#[tokio::test]
async fn only_a_wiped_learner_learns_a_set_without_the_set_before_it() {
    for (learner, wiped, learns) in [
        (true, true, true),
        (false, true, false),
        (true, false, false),
    ] {
        let (broker, _dir) = broker_with_storage().await;
        stream_log(&broker)
            .await
            .accept_set(ReplicaSet {
                current: Membership {
                    generation: 3,
                    members: vec![Member {
                        node: LOCAL.to_string(),
                        storage: None,
                        learner,
                        wiped,
                    }],
                },
                leaving: None,
                earlier: Vec::new(),
            })
            .await
            .expect("set");
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_after(4, None))
            .with_fleet(joint_marks())
            .with_confirmed_routes(Arc::new(AssignedToSender(4)));
        let answer = settled(|| {
            handler.apply(
                SENDER,
                stamped(4, 0, &["a"], 4),
                felix_broker::LogKind::Stream,
            )
        })
        .await;
        let stale = refused_for_set_before(&answer);
        assert_eq!(
            stale, !learns,
            "learner {learner}, wiped {wiped}: {answer:?}"
        );
        if learns {
            assert!(
                matches!(answer, InternalMessage::ReplicateOk(_)),
                "{answer:?}"
            );
            assert_eq!(stream_log(&broker).await.set_generation(), 4);
        } else {
            assert_eq!(stream_log(&broker).await.set_generation(), 3);
        }
    }
}

/// **A fence counting an older set is refused, naming the newer one**, and
/// takes no ballot: a sender that has not seen the latest set must not gather
/// a majority from replicas that have. `RefuseStaleSet`.
#[tokio::test]
async fn a_fence_on_an_older_set_is_refused_and_names_the_newer() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    handler
        .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    knows_set(&broker, 5).await;

    let answer = handler
        .fence(SENDER, election_fence(6, voters(3), false))
        .await;

    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::StaleSet);
    assert_eq!(refused.expected_offset, 5);
    assert!(!refused.code.is_retryable());
    let log = stream_log(&broker).await;
    assert_eq!(
        log.accepted_generation(),
        4,
        "the stale fence took a ballot"
    );
    assert_eq!(log.set_generation(), 5);
}

/// A batch stamped with an older set is refused the same way, and stores
/// nothing.
#[tokio::test]
async fn a_batch_stamped_with_an_older_set_is_refused() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    handler
        .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    knows_set(&broker, 5).await;

    let answer = handler
        .apply(
            SENDER,
            stamped(4, 1, &["b"], 3),
            felix_broker::LogKind::Stream,
        )
        .await;

    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::StaleSet);
    assert_eq!(refused.expected_offset, 5);
    assert_eq!(held(&broker).await, vec!["a"]);
}

/// A batch stamped with a newer set is taken when the routing view has that
/// generation, and the set is learned from it; one the view does not have is
/// refused for now, for a fence to carry it.
#[tokio::test]
async fn a_batch_stamped_with_a_newer_set_learns_it_from_the_route() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));

    let unknown = handler
        .apply(
            SENDER,
            stamped(4, 0, &["a"], 5),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert_eq!(refusal(&unknown).code, ErrorCode::StaleRoute);

    let answer = handler
        .apply(
            SENDER,
            stamped(4, 0, &["a"], 4),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let set = stream_log(&broker)
        .await
        .replica_set()
        .expect("the set was learned");
    assert_eq!(set.current.generation, 4);
    let nodes: Vec<&str> = set
        .current
        .members
        .iter()
        .map(|member| member.node.as_str())
        .collect();
    assert_eq!(nodes, vec!["broker-a", LOCAL]);
}

/// **The set is on disk before the fence is answered**, so a restart cannot
/// answer an older set this replica already counted past.
#[tokio::test]
async fn the_set_is_on_disk_before_the_fence_is_answered() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
        ok(&handler
            .fence(SENDER, election_fence(5, voters(5), false))
            .await);
    }

    // Reopened without a shutdown.
    let broker = broker_on(dir.path());
    let log = stream_log(&broker).await;
    assert_eq!(log.set_generation(), 5, "the answered set was not on disk");
    assert_eq!(log.replica_set().expect("set").current.members.len(), 3);
}

/// **A replica that hears its leader refuses a candidate**, until it has
/// gone the stickiness window without hearing it. The leader's own fence is
/// not held back.
#[tokio::test]
async fn a_replica_that_hears_its_leader_refuses_a_candidate() {
    let (broker, _dir) = broker_with_storage().await;
    let stickiness = Duration::from_millis(200);
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4))
        .with_stickiness(stickiness);
    let answer = handler
        .apply(
            SENDER,
            stamped(4, 0, &["a"], 4),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );

    let candidate = Some("broker-c");
    let answer = handler
        .fence(candidate, election_fence(5, voters(5), true))
        .await;
    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::Unavailable);
    assert_eq!(
        stream_log(&broker).await.accepted_generation(),
        4,
        "a refused candidate took a ballot"
    );

    // The leader's own fence is not a candidate's, so stickiness does not
    // apply to it; it also counts as hearing the leader.
    ok(&handler
        .fence(SENDER, election_fence(5, voters(5), false))
        .await);

    tokio::time::sleep(stickiness + Duration::from_millis(50)).await;
    ok(&handler
        .fence(candidate, election_fence(6, voters(6), true))
        .await);
}

/// **What the stickiness hears is what per-shard suspicion hears**: a
/// stamped batch stored and a leader's fence taken, but not a candidate's
/// fence. Otherwise a copy could suspect a leader its voters still hear, or
/// the other way round.
#[tokio::test]
async fn a_replica_notes_hearing_its_leader_for_shard_suspicion() {
    let (broker, _dir) = broker_with_storage().await;
    let marks = Arc::new(crate::quorum::QuorumMarks::new());
    let elections = Arc::new(crate::election::Elections::new(
        LOCAL,
        Arc::new(tokio::sync::Notify::new()),
    ));
    marks.set_elections(Arc::clone(&elections));
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6))
        .with_stickiness(Duration::ZERO)
        .with_fleet(marks);
    let key = felix_router::ShardKey {
        tenant_id: TENANT.into(),
        namespace: NAMESPACE.into(),
        stream: STREAM.into(),
        shard: 0,
        kind: felix_router::ShardKind::Stream,
    };

    ok(&handler
        .fence(SENDER, election_fence(6, voters(6), false))
        .await);
    let fenced = elections.last_heard(&key).expect("a leader's fence");

    tokio::time::sleep(Duration::from_millis(5)).await;
    let answer = handler
        .apply(
            SENDER,
            stamped(6, 0, &["a"], 6),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let stored = elections.last_heard(&key).expect("a batch");
    assert!(stored > fenced);

    tokio::time::sleep(Duration::from_millis(5)).await;
    ok(&handler
        .fence(Some("broker-c"), election_fence(7, voters(7), true))
        .await);
    assert_eq!(
        elections.last_heard(&key),
        Some(stored),
        "a candidate's fence"
    );
}

/// Stickiness is judged on tokio's clock, so a simulation on virtual time
/// sees a silent leader's window close.
#[tokio::test(start_paused = true)]
async fn stickiness_runs_on_tokio_time() {
    let (broker, _dir) = broker_with_storage().await;
    let stickiness = Duration::from_secs(60);
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4))
        .with_stickiness(stickiness);
    handler
        .apply(
            SENDER,
            stamped(4, 0, &["a"], 4),
            felix_broker::LogKind::Stream,
        )
        .await;
    let candidate = Some("broker-c");
    let answer = handler
        .fence(candidate, election_fence(5, voters(5), true))
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unavailable);

    tokio::time::advance(stickiness).await;
    ok(&handler
        .fence(candidate, election_fence(5, voters(5), true))
        .await);
}

/// **A broker that does not vote in the set refuses its fence**: listed only
/// as a learner (#1331), or not listed at all. Its answer must not count
/// toward the sender's majority, and it takes no ballot.
#[tokio::test]
async fn a_replica_that_does_not_vote_in_the_set_refuses_the_fence() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    handler
        .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;

    for set in [
        view(5, &["broker-a", "broker-c"], &[LOCAL]),
        view(5, &["broker-a", "broker-c"], &[]),
    ] {
        let answer = handler.fence(SENDER, election_fence(5, set, false)).await;
        assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    }
    let log = stream_log(&broker).await;
    assert_eq!(log.accepted_generation(), 4);
    assert_eq!(log.set_generation(), 0);

    // A member of the set being left still votes while the change is pending.
    let answer = handler
        .fence(
            SENDER,
            Fence {
                election: Some(FenceElection {
                    candidate: false,
                    set: view(6, &["broker-a", "broker-c"], &[]),
                    leaving: Some(voters(5)),
                    earlier: Vec::new(),
                }),
                ..election_fence(6, voters(6), false)
            },
        )
        .await;
    ok(&answer);
}

/// Without a set nothing is checked or kept: the fence a broker without
/// `ELECTION` is sent behaves as it did.
#[tokio::test]
async fn a_fence_without_a_set_keeps_none() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    knows_set(&broker, 9).await;
    let answer = handler
        .fence(
            SENDER,
            Fence {
                election: None,
                ..election_fence(5, voters(5), false)
            },
        )
        .await;
    ok(&answer);
    assert_eq!(stream_log(&broker).await.set_generation(), 9);
}

/// **A set newer than the fence's own generation is refused**, taking no
/// ballot and keeping nothing: a sender stamps a route generation no newer
/// than its fence's, and keeping the set would have the real leader's batches
/// refused as `StaleSet`. Unauthorized, so nobody is deposed.
#[tokio::test]
async fn a_fence_carrying_a_set_ahead_of_its_generation_is_refused() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    handler
        .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;

    let answer = handler
        .fence(SENDER, election_fence(5, voters(9), false))
        .await;

    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = stream_log(&broker).await;
    assert_eq!(log.accepted_generation(), 4);
    assert_eq!(log.set_generation(), 0);
    // A set at the fence's own generation is still taken.
    ok(&handler
        .fence(SENDER, election_fence(5, voters(5), false))
        .await);
}

/// A batch stamped with a set this replica does not know, at another
/// generation than the batch's own, is refused for now: no route has that
/// set, so only a fence can carry it.
#[tokio::test]
async fn a_batch_stamped_with_an_unknown_set_at_another_generation_is_refused() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6));
    handler
        .apply(SENDER, batch(6, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    knows_set(&broker, 3).await;

    let answer = handler
        .apply(
            SENDER,
            stamped(6, 1, &["b"], 5),
            felix_broker::LogKind::Stream,
        )
        .await;

    assert_eq!(refusal(&answer).code, ErrorCode::StaleRoute);
    assert!(refusal(&answer).code.is_retryable());
    assert_eq!(stream_log(&broker).await.set_generation(), 3);
    assert_eq!(held(&broker).await, vec!["a"]);
}

/// **An elected leader's batch is taken.** It stamps the set it stood on,
/// which is older than the generation it leads, since standing does not
/// change the set. The replica keeps that set, so there is nothing to learn
/// (`SetCurrent`).
#[tokio::test]
async fn a_batch_stamped_with_the_set_this_replica_keeps_is_taken() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6));
    handler
        .apply(SENDER, batch(6, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    knows_set(&broker, 5).await;

    let answer = handler
        .apply(
            SENDER,
            stamped(6, 1, &["b"], 5),
            felix_broker::LogKind::Stream,
        )
        .await;

    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "expected the batch to be stored, got {answer:?}"
    );
    assert_eq!(stream_log(&broker).await.set_generation(), 5);
    assert_eq!(held(&broker).await, vec!["a", "b"]);
}

fn pre_vote_at(generation: u64) -> felix_wire::internal::PreVote {
    felix_wire::internal::PreVote {
        correlation_id: 3,
        shard: batch(generation, 0, &[]).shard,
        log: ReplicaLog::Stream,
        last_generation: 0,
        log_end: 0,
        set_generation: 0,
    }
}

/// **The leader refuses a pre-vote against itself** from a follower whose log
/// equals its own (#1815). A follower that has not heard a leader grants it.
#[tokio::test]
async fn the_leader_refuses_a_pre_vote_against_itself() {
    for (leader, refusal) in [
        (
            LOCAL,
            Some(felix_wire::internal::PreVoteRefusal::LeaderReachable),
        ),
        ("broker-a", None),
    ] {
        let (broker, _dir) = broker_with_storage().await;
        if leader == LOCAL {
            opened_at(&broker, 6).await;
        }
        let others: Vec<&str> = ["broker-a", LOCAL, "broker-c"]
            .into_iter()
            .filter(|node| *node != leader)
            .collect();
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_led_by(leader, &others, 6))
            .with_stickiness(Duration::ZERO);
        let answer = handler.pre_vote(pre_vote_at(7)).await;
        let InternalMessage::PreVoteAnswer(answer) = answer else {
            panic!("expected an answer, got {answer:?}");
        };
        assert_eq!(answer.refusal, refusal, "led by {leader}");
    }
}

/// This broker's log as a leader's that opened at `generation`.
async fn opened_at(broker: &Broker, generation: u64) {
    let log = stream_log(broker).await;
    log.accept_generation(generation, Some(LOCAL))
        .await
        .expect("ballot");
    begin(&log, generation).await;
    log.append_generation_start(generation)
        .await
        .expect("start record");
}

/// Note where `generation` begins, as the lifecycle's open does.
async fn begin(log: &felix_broker::StreamLog, generation: u64) {
    let tail = log.tail_offset().await.expect("tail");
    log.record_generation(generation, tail).expect("begins");
}

fn pre_vote_answer(answer: InternalMessage) -> Option<felix_wire::internal::PreVoteRefusal> {
    let InternalMessage::PreVoteAnswer(answer) = answer else {
        panic!("expected an answer, got {answer:?}");
    };
    answer.refusal
}

/// **A route naming this broker leader refuses a pre-vote only while the
/// shard is open here at its generation.** A route can name it long after it
/// stopped serving, and refusing then keeps a live majority from electing.
#[tokio::test]
async fn only_a_leader_open_at_its_routes_generation_refuses_a_pre_vote() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(
        Arc::clone(&broker),
        router_led_by(LOCAL, &["broker-a", "broker-c"], 6),
    )
    .with_stickiness(Duration::ZERO);
    stream_log(&broker)
        .await
        .accept_generation(6, Some(LOCAL))
        .await
        .expect("ballot");
    assert_eq!(
        pre_vote_answer(handler.pre_vote(pre_vote_at(7)).await),
        None
    );

    begin(&stream_log(&broker).await, 6).await;
    assert_eq!(
        pre_vote_answer(handler.pre_vote(pre_vote_at(7)).await),
        Some(felix_wire::internal::PreVoteRefusal::LeaderReachable)
    );
}

/// **A leader cut off while the others elected takes the elected leader's
/// fence when it comes again**, records that leader, and then grants a
/// pre-vote once it goes quiet. Before the fence it refuses the new leader's
/// batches as behind, since no route names the elected generation, and
/// refuses pre-votes as the leader its stale route names. This is the
/// follower's side; the leader sending the fence again is tested in the
/// driver.
#[tokio::test]
async fn a_cut_off_leader_takes_the_elected_leaders_fence() {
    let (broker, _dir) = broker_with_storage().await;
    let marks = Arc::new(crate::quorum::QuorumMarks::new());
    let elections = Arc::new(crate::election::Elections::new(
        LOCAL,
        Arc::new(tokio::sync::Notify::new()),
    ));
    marks.set_elections(Arc::clone(&elections));
    let handler = ReplicaHandler::new(
        Arc::clone(&broker),
        router_led_by(LOCAL, &["broker-a", "broker-c"], 6),
    )
    .with_stickiness(Duration::ZERO)
    .with_fleet(marks);
    opened_at(&broker, 6).await;
    let elected = Some("broker-a");

    let answer = handler
        .apply(
            elected,
            stamped(7, 1, &["x"], 6),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::StaleRoute);
    let at_8 = || felix_wire::internal::PreVote {
        last_generation: 6,
        log_end: 1,
        set_generation: 6,
        ..pre_vote_at(8)
    };
    assert_eq!(
        pre_vote_answer(handler.pre_vote(at_8()).await),
        Some(felix_wire::internal::PreVoteRefusal::LeaderReachable)
    );

    ok(&handler
        .fence(elected, election_fence(7, voters(6), true))
        .await);
    let log = stream_log(&broker).await;
    assert_eq!(log.accepted_generation(), 7);
    assert_eq!(log.accepted_leader().as_deref(), Some("broker-a"));
    let recorded = elections.get(&key()).expect("the elected leader");
    assert_eq!(
        (
            recorded.leader.as_str(),
            recorded.generation,
            recorded.set.generation
        ),
        ("broker-a", 7, 6)
    );
    assert_eq!(pre_vote_answer(handler.pre_vote(at_8()).await), None);
}

/// **Only a candidate's fence that is taken records its sender**: a fence
/// from a leader the control plane named records nothing, and neither does a
/// refused candidate.
#[tokio::test]
async fn only_a_taken_candidate_fence_records_its_sender() {
    let (broker, _dir) = broker_with_storage().await;
    let marks = Arc::new(crate::quorum::QuorumMarks::new());
    let elections = Arc::new(crate::election::Elections::new(
        LOCAL,
        Arc::new(tokio::sync::Notify::new()),
    ));
    marks.set_elections(Arc::clone(&elections));
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4))
        .with_stickiness(Duration::ZERO)
        .with_fleet(marks);

    ok(&handler
        .fence(SENDER, election_fence(5, voters(5), false))
        .await);
    assert!(elections.get(&key()).is_none(), "a named leader's fence");

    // The set is older than the one this replica now keeps.
    let answer = handler
        .fence(Some("broker-c"), election_fence(6, voters(4), true))
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::StaleSet);
    assert!(elections.get(&key()).is_none(), "a refused candidate");

    ok(&handler
        .fence(Some("broker-c"), election_fence(6, voters(5), true))
        .await);
    assert_eq!(
        elections.get(&key()).map(|elected| elected.leader),
        Some("broker-c".to_string())
    );
}

/// **A leader fencing or just opened at the generation it won refuses a
/// pre-vote against itself** (#1914). It has no start record yet, but a yes then lets a
/// follower it just fenced, whose third replica is gone, take the next
/// generation before the leader serves. Once the fence ends, a route alone
/// refuses nothing again.
#[tokio::test]
async fn a_leader_still_fencing_refuses_a_pre_vote() {
    let (broker, _dir) = broker_with_storage().await;
    stream_log(&broker)
        .await
        .accept_generation(6, Some(LOCAL))
        .await
        .expect("ballot");
    let marks = Arc::new(crate::quorum::QuorumMarks::new());
    let elections = Arc::new(crate::election::Elections::new(
        LOCAL,
        Arc::new(tokio::sync::Notify::new()),
    ));
    marks.set_elections(Arc::clone(&elections));
    let router = router_led_by(LOCAL, &["broker-a", "broker-c"], 6);
    let key = router.snapshot().iter().next().expect("key").0.clone();
    let handler = ReplicaHandler::new(Arc::clone(&broker), router)
        .with_stickiness(Duration::ZERO)
        .with_fleet(marks);
    let fencing = elections.fencing(&key, 6);
    assert_eq!(
        pre_vote_answer(handler.pre_vote(pre_vote_at(7)).await),
        Some(felix_wire::internal::PreVoteRefusal::LeaderReachable)
    );
    drop(fencing);
    assert_eq!(
        pre_vote_answer(handler.pre_vote(pre_vote_at(7)).await),
        None
    );

    // Opened on the standing, with no start record yet.
    elections.record(
        key.clone(),
        crate::election::Elected {
            leader: LOCAL.to_string(),
            generation: 6,
            set: felix_storage::disk_log::Membership {
                generation: 6,
                members: Vec::new(),
            },
            leaving: None,
            incarnation: 0,
        },
    );
    elections.opened(&key, 6);
    assert_eq!(
        pre_vote_answer(handler.pre_vote(pre_vote_at(7)).await),
        Some(felix_wire::internal::PreVoteRefusal::LeaderReachable)
    );
}

/// **A standing that never starts its generation stops refusing pre-votes**
/// once the defer bound has passed since its first fence (#1914): stuck
/// fencing or opened without a start record, it would otherwise keep its
/// followers from electing anyone until the control plane is back.
#[tokio::test(start_paused = true)]
async fn a_standing_that_never_starts_stops_refusing_pre_votes() {
    let (broker, _dir) = broker_with_storage().await;
    stream_log(&broker)
        .await
        .accept_generation(6, Some(LOCAL))
        .await
        .expect("ballot");
    let marks = Arc::new(crate::quorum::QuorumMarks::new());
    let elections = Arc::new(crate::election::Elections::new(
        LOCAL,
        Arc::new(tokio::sync::Notify::new()),
    ));
    marks.set_elections(Arc::clone(&elections));
    let router = router_led_by(LOCAL, &["broker-a", "broker-c"], 6);
    let key = router.snapshot().iter().next().expect("key").0.clone();
    let handler = ReplicaHandler::new(Arc::clone(&broker), router)
        .with_stickiness(Duration::ZERO)
        .with_fleet(marks);
    elections.record(
        key.clone(),
        crate::election::Elected {
            leader: LOCAL.to_string(),
            generation: 6,
            set: felix_storage::disk_log::Membership {
                generation: 6,
                members: Vec::new(),
            },
            leaving: None,
            incarnation: 0,
        },
    );
    let fencing = elections.fencing(&key, 6);
    tokio::time::advance(elections.defer_for() / 2).await;
    drop(fencing);
    // A fence retried keeps its first time.
    let _fencing = elections.fencing(&key, 6);
    assert_eq!(
        pre_vote_answer(handler.pre_vote(pre_vote_at(7)).await),
        Some(felix_wire::internal::PreVoteRefusal::LeaderReachable)
    );
    tokio::time::advance(elections.defer_for() / 2).await;
    assert_eq!(
        pre_vote_answer(handler.pre_vote(pre_vote_at(7)).await),
        None
    );

    elections.opened(&key, 6);
    assert_eq!(
        pre_vote_answer(handler.pre_vote(pre_vote_at(7)).await),
        None
    );
}

/// **A standing refuses pre-votes from the moment it is recorded**, before
/// the driver reaches its fence (#1914). In that gap a follower that voted
/// for it would otherwise win the next generation with its yes.
#[tokio::test]
async fn a_standing_not_yet_fencing_refuses_a_pre_vote() {
    let (broker, _dir) = broker_with_storage().await;
    stream_log(&broker)
        .await
        .accept_generation(6, Some(LOCAL))
        .await
        .expect("ballot");
    let marks = Arc::new(crate::quorum::QuorumMarks::new());
    let elections = Arc::new(crate::election::Elections::new(
        LOCAL,
        Arc::new(tokio::sync::Notify::new()),
    ));
    marks.set_elections(Arc::clone(&elections));
    let router = router_led_by(LOCAL, &["broker-a", "broker-c"], 6);
    let key = router.snapshot().iter().next().expect("key").0.clone();
    let handler = ReplicaHandler::new(Arc::clone(&broker), router)
        .with_stickiness(Duration::ZERO)
        .with_fleet(marks);
    elections.record(
        key.clone(),
        crate::election::Elected {
            leader: LOCAL.to_string(),
            generation: 6,
            set: felix_storage::disk_log::Membership {
                generation: 6,
                members: Vec::new(),
            },
            leaving: None,
            incarnation: 0,
        },
    );
    assert_eq!(
        pre_vote_answer(handler.pre_vote(pre_vote_at(7)).await),
        Some(felix_wire::internal::PreVoteRefusal::LeaderReachable)
    );
    elections.withdraw(&key, 6);
    assert_eq!(
        pre_vote_answer(handler.pre_vote(pre_vote_at(7)).await),
        None
    );
}

/// **A set from a fence or batch below a ballot taken since is refused**
/// (#1970): a candidate's ballot can land between the sender's ballot and
/// its set, and the candidate stood on the set the copy kept then.
#[tokio::test]
async fn a_set_from_a_sender_promised_past_is_not_kept() {
    let (broker, _dir) = broker_with_storage().await;
    let log = stream_log(&broker).await;
    knows_set(&broker, 3).await;
    // The sender's ballot, then a candidate's above it.
    log.accept_generation(4, SENDER).await.expect("ballot");
    log.accept_generation(6, Some(LOCAL)).await.expect("ballot");
    let before = log.replica_set();

    let member = |node: &str| Member {
        node: node.to_string(),
        storage: None,
        learner: false,
        wiped: false,
    };
    let set = ReplicaSet {
        current: Membership {
            generation: 4,
            members: vec![member("broker-a"), member(LOCAL), member("broker-d")],
        },
        leaving: before.as_ref().map(|kept| kept.current.clone()),
        earlier: Vec::new(),
    };
    let answer = super::super::election::keep(9, &key(), &log, set, false, (4, SENDER))
        .await
        .expect("refused");
    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch);
    assert_eq!(log.replica_set(), before, "the set is unchanged");
}
