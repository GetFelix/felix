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
            }],
        },
        leaving: None,
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

/// **A batch is stamped with its own generation**: a stamp that names another
/// set is refused as retryable, even when the routing view has it, and the
/// batch stores nothing.
#[tokio::test]
async fn a_batch_stamped_with_another_generation_than_its_own_is_refused() {
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

    assert_eq!(refusal(&answer).code, ErrorCode::StaleRoute);
    assert!(refusal(&answer).code.is_retryable());
    assert_eq!(stream_log(&broker).await.set_generation(), 5);
    assert_eq!(held(&broker).await, vec!["a"]);
}
