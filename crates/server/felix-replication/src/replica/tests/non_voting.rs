//! A copy that lost its storage votes in nothing until a set change
//! re-admits it (#1331): `NonVoter`, `Readmit`, `Readmitted` and `Creates` in
//! `docs/formal/FelixShard.tla`. Only a set naming another storage id for the
//! copy is evidence of the wipe.
use std::time::Duration;

use felix_common::fleet::{FleetGate, JOINT_FENCE};
use felix_storage::StorageId;
use felix_wire::internal::{
    Fence, FenceElection, GenerationStart, PreVote, PreVoteRefusal, ReplicaLog, ReplicaSetView,
    SetMember,
};

use super::*;
use crate::confirmed::Confirmed;
use crate::quorum::QuorumMarks;

/// This broker's directory now, and the one a wipe replaced.
const OWN: [u8; 16] = [7; 16];
const OLD: [u8; 16] = [3; 16];

/// The gate as a registration's answer left it.
fn marks(joint: bool) -> Arc<QuorumMarks> {
    let fleet = FleetGate::new([JOINT_FENCE.name()]);
    fleet.restart(joint.then_some(JOINT_FENCE.name()));
    Arc::new(QuorumMarks::with_fleet(Arc::new(fleet)))
}

/// A handler on `broker` under `joint_fence`, whose copy holds `own`, with the
/// control plane confirming `generation` led by broker-a.
fn handler(broker: &Arc<Broker>, generation: u64, own: [u8; 16]) -> ReplicaHandler {
    ReplicaHandler::new(Arc::clone(broker), router_with(&[LOCAL], generation))
        .with_fleet(marks(true))
        .with_local_storage(StorageId::from_bytes(own))
        .with_stickiness(Duration::ZERO)
        .with_confirmed_routes(ConfirmedAs::new(Confirmed::Assigned {
            generation,
            leader: "broker-a".to_string(),
        }) as _)
}

/// The control plane answering a confirmed read.
struct ConfirmedAs(Confirmed);

impl ConfirmedAs {
    fn new(answer: Confirmed) -> Arc<Self> {
        Arc::new(Self(answer))
    }
}

#[async_trait::async_trait]
impl crate::confirmed::ConfirmedRoutes for ConfirmedAs {
    async fn confirmed(&self, _key: &felix_router::ShardKey) -> Confirmed {
        self.0.clone()
    }
}

/// A set at `generation` that names this broker under `storage`.
fn set(generation: u64, storage: [u8; 16]) -> ReplicaSetView {
    named(generation, Some(storage))
}

/// A set at `generation`, naming this broker's storage as `storage` and the
/// others' only when it names this one's: the control plane names them all
/// on a failover and none elsewhere.
fn named(generation: u64, storage: Option<[u8; 16]>) -> ReplicaSetView {
    let member = |node: &str, id: [u8; 16]| SetMember {
        node_id: node.to_string(),
        storage_id: storage.map(|_| id),
        learner: false,
    };
    let storage = storage.unwrap_or_default();
    ReplicaSetView {
        generation,
        members: vec![
            member("broker-a", [1; 16]),
            member(LOCAL, storage),
            member("broker-c", [2; 16]),
        ],
    }
}

fn fence_with(
    generation: u64,
    set: ReplicaSetView,
    leaving: Option<ReplicaSetView>,
    candidate: bool,
) -> Fence {
    Fence {
        correlation_id: 9,
        shard: batch(generation, 0, &[]).shard,
        log: ReplicaLog::Stream,
        election: Some(FenceElection {
            candidate,
            set,
            leaving,
        }),
    }
}

/// The fence of a re-admission at `generation`: a joint one.
fn readmission(generation: u64, storage: [u8; 16]) -> Fence {
    fence_with(
        generation,
        set(generation, storage),
        Some(set(generation - 1, OLD)),
        false,
    )
}

fn stamped(generation: u64, first_offset: u64, values: &[&str]) -> ReplicateRecords {
    ReplicateRecords {
        set_generation: Some(generation),
        generations: Some(vec![GenerationStart {
            generation,
            start_offset: first_offset,
        }]),
        ..batch(generation, first_offset, values)
    }
}

async fn log_of(broker: &Broker) -> felix_broker::StreamLog {
    broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
}

fn pre_vote(generation: u64) -> PreVote {
    PreVote {
        correlation_id: 4,
        shard: batch(generation, 0, &[]).shard,
        log: ReplicaLog::Stream,
        last_generation: 0,
        log_end: 0,
        set_generation: 0,
    }
}

/// Teach a wiped copy a set naming `OLD` for it, as a refused fence does.
async fn wiped_at_5(handler: &ReplicaHandler) {
    let answer = handler
        .fence(SENDER, fence_with(6, set(5, OLD), None, false))
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
}

/// **A wiped copy refuses a fence and a candidate's vote**, takes no ballot,
/// and still learns the set the fence carried, as a learner.
#[tokio::test]
async fn a_wiped_copy_refuses_a_fence_and_a_vote() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, OWN);

    for candidate in [false, true] {
        let answer = handler
            .fence(SENDER, fence_with(6, set(5, OLD), None, candidate))
            .await;
        assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    }
    let log = log_of(&broker).await;
    assert_eq!(
        log.accepted_generation(),
        0,
        "a refused fence took a ballot"
    );
    assert_eq!(log.set_generation(), 5, "the set was not learned");
    assert_eq!(log.member_is_learner(LOCAL), Some(true));
}

/// A copy that votes is not held back by the same rule: the guard is on the
/// non-voter, not on every fence.
#[tokio::test]
async fn a_voting_copy_still_answers_a_candidate() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, OWN);
    let log = log_of(&broker).await;
    log.accept_set(felix_storage::disk_log::ReplicaSet {
        current: crate::replica_set::membership(&set(5, OWN)),
        leaving: None,
    })
    .await
    .expect("set");
    assert_eq!(log.member_is_learner(LOCAL), Some(false));

    let answer = handler
        .fence(SENDER, fence_with(6, set(5, OWN), None, true))
        .await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
}

/// **A wiped copy answers a pre-vote** and writes nothing, but its answer
/// is a refusal no asker counts.
#[tokio::test]
async fn a_wiped_copy_answers_a_pre_vote_without_voting() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, OWN);
    wiped_at_5(&handler).await;

    let answer = handler.pre_vote(pre_vote(7)).await;
    let InternalMessage::PreVoteAnswer(answer) = answer else {
        panic!("expected an answer, got {answer:?}");
    };
    assert_eq!(answer.refusal, Some(PreVoteRefusal::NotVoting));
    assert_eq!(log_of(&broker).await.accepted_generation(), 0);
}

/// **A restart keeps a copy non-voting**: the learner mark is on disk.
#[tokio::test]
async fn a_restart_keeps_a_wiped_copy_non_voting() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let broker = broker_on(dir.path());
        wiped_at_5(&handler(&broker, 6, OWN)).await;
    }
    let broker = broker_on(dir.path());
    assert_eq!(log_of(&broker).await.member_is_learner(LOCAL), Some(true));
    let handler = handler(&broker, 6, OWN);
    let answer = handler
        .fence(SENDER, fence_with(6, set(5, OWN), None, true))
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
}

/// **A re-admission naming another storage id does not re-admit.** The set
/// change was made for the copy a report described; after a second wipe the
/// directory is another one, and an empty copy must not take its place.
#[tokio::test]
async fn a_readmission_for_another_storage_id_does_not_readmit() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, [9; 16]);
    wiped_at_5(&handler).await;

    let answer = handler.fence(SENDER, readmission(6, OWN)).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert_eq!(log.accepted_generation(), 0);

    // Not even catching up admits it.
    let answer = handler
        .apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(log.member_is_learner(LOCAL), Some(true));
}

/// **A set naming its storage id re-admits it, once it has caught up.** The
/// fence carrying it is refused, so no leader counts the copy before then;
/// the start of the set's generation reaching its log ends the wait.
#[tokio::test]
async fn a_set_naming_its_storage_id_readmits_it_after_catch_up() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, OWN);
    wiped_at_5(&handler).await;

    let answer = handler.fence(SENDER, readmission(6, OWN)).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert_eq!(log.set_generation(), 6, "the set was not learned");
    assert_eq!(log.member_is_learner(LOCAL), Some(true));

    let answer = handler
        .apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(log.member_is_learner(LOCAL), Some(false));
}

/// **A learner whose re-admitting leader is lost is re-admitted by the next
/// one.** The set naming it at 6 was learned, its leader never shipped, and
/// generation 7 comes with a plain fence: the copy catches up with 7 instead.
#[tokio::test]
async fn a_stranded_learner_is_readmitted() {
    let (broker, _dir) = broker_with_storage().await;
    let at_6 = handler(&broker, 6, OWN);
    wiped_at_5(&at_6).await;
    let answer = at_6.fence(SENDER, readmission(6, OWN)).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);

    let handler = handler(&broker, 7, OWN);
    let answer = handler
        .fence(SENDER, fence_with(7, set(7, OWN), None, false))
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert_eq!(log.set_generation(), 7);

    let answer = handler
        .apply(SENDER, stamped(7, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(log.member_is_learner(LOCAL), Some(false), "stranded");
}

/// **A new shard opens under the gate.** Its copies hold no set and no
/// promise, and the creating fence names no storage ids: that is no evidence
/// of a wipe, so they vote and take the first batch.
#[tokio::test]
async fn a_new_shard_opens_under_the_gate() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 1, OWN);

    let answer = handler
        .fence(SENDER, fence_with(1, named(1, None), None, false))
        .await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    let log = log_of(&broker).await;
    assert_eq!(log.member_is_learner(LOCAL), Some(false));
    let answer = handler
        .apply(SENDER, stamped(1, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
}

/// **A copy from before sets stays a voter at finalize.** It holds records
/// and a ballot but no set file; plain fences, the read round's confirming
/// one, and a set naming no storage ids are all answered.
#[tokio::test]
async fn a_pre_set_copy_stays_a_voter_at_finalize() {
    let (broker, _dir) = broker_with_storage().await;
    let before = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5))
        .with_fleet(marks(false))
        .with_local_storage(StorageId::from_bytes(OWN));
    let answer = before
        .apply(SENDER, batch(5, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let log = log_of(&broker).await;
    assert!(log.replica_set().is_none());

    let plain = |generation| Fence {
        election: None,
        ..fence_with(generation, named(generation, None), None, false)
    };
    // The read round's confirming fence, then the next failover's.
    for generation in [5, 6] {
        let answer = handler(&broker, generation, OWN)
            .fence(SENDER, plain(generation))
            .await;
        assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    }
    let answer = handler(&broker, 7, OWN)
        .fence(SENDER, fence_with(7, named(7, None), None, false))
        .await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    assert_eq!(log.member_is_learner(LOCAL), Some(false));
}

/// **Before the gate is read, a wiped copy refuses** what the gate decides,
/// retryably and writing nothing. A copy with no evidence of a wipe answers,
/// so a broker restarting with its data serves before it registers.
#[tokio::test]
async fn a_wiped_copy_refuses_before_the_gate_is_adopted() {
    let (broker, _dir) = broker_with_storage().await;
    let fleet = Arc::new(FleetGate::new([JOINT_FENCE.name()]));
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6))
        .with_fleet(Arc::new(QuorumMarks::with_fleet(Arc::clone(&fleet))))
        .with_local_storage(StorageId::from_bytes(OWN))
        .with_stickiness(Duration::ZERO)
        .with_confirmed_routes(ConfirmedAs::new(Confirmed::Assigned {
            generation: 6,
            leader: "broker-a".to_string(),
        }) as _);

    let answer = handler
        .fence(SENDER, fence_with(6, set(5, OLD), None, false))
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unavailable);
    let log = log_of(&broker).await;
    assert_eq!(log.set_generation(), 0, "wrote before the gate was read");
    assert_eq!(log.accepted_generation(), 0);

    let answer = handler
        .fence(SENDER, fence_with(6, named(5, None), None, false))
        .await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");

    fleet.restart([JOINT_FENCE.name()]);
    let answer = handler
        .fence(SENDER, fence_with(7, set(6, OLD), None, false))
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    assert_eq!(log.member_is_learner(LOCAL), Some(true));
}

/// A learner asks the control plane once for a run of batches at one
/// generation, not once per batch.
#[tokio::test]
async fn a_learner_reuses_a_confirmed_read() {
    let (broker, _dir) = broker_with_storage().await;
    let reads = Arc::new(CountingConfirmed::default());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6))
        .with_fleet(marks(true))
        .with_local_storage(StorageId::from_bytes([9; 16]))
        .with_confirmed_routes(Arc::clone(&reads) as _);
    wiped_at_5(&handler).await;

    let before = reads.count();
    for (offset, value) in [(0, "a"), (2, "b")] {
        let answer = handler
            .apply(
                SENDER,
                stamped(6, offset, &[value, value]),
                felix_broker::LogKind::Stream,
            )
            .await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "{answer:?}"
        );
    }
    assert_eq!(reads.count() - before, 1);
}

/// The control plane confirming broker-a at 6, counting the reads.
#[derive(Default)]
struct CountingConfirmed(std::sync::atomic::AtomicUsize);

impl CountingConfirmed {
    fn count(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl crate::confirmed::ConfirmedRoutes for CountingConfirmed {
    async fn confirmed(&self, _key: &felix_router::ShardKey) -> Confirmed {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Confirmed::Assigned {
            generation: 6,
            leader: "broker-a".to_string(),
        }
    }
}

/// A learner takes records only from the leader the control plane confirms,
/// even once it holds a promise: here the view has reached an elected leader
/// the control plane has not, and taking its records would acknowledge them.
#[tokio::test]
async fn a_wiped_copy_refuses_records_from_an_unconfirmed_leader() {
    let (broker, _dir) = broker_with_storage().await;
    wiped_at_5(&handler(&broker, 6, OWN)).await;
    let log = log_of(&broker).await;
    log.accept_generation(6, Some("broker-a"))
        .await
        .expect("ballot");

    let lagging = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 7))
        .with_fleet(marks(true))
        .with_local_storage(StorageId::from_bytes(OWN))
        .with_confirmed_routes(ConfirmedAs::new(Confirmed::Assigned {
            generation: 6,
            leader: "broker-a".to_string(),
        }) as _);
    let answer = lagging
        .apply(SENDER, stamped(7, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateError(_)),
        "{answer:?}"
    );
    assert_eq!(log.tail_offset().await.expect("tail"), 0);
}

/// A copy with no promise does not take a generation newer than the control
/// plane's under `joint_fence`: a candidate it never promised to can hold it.
/// Without the feature it answers as before.
#[tokio::test]
async fn a_copy_with_no_promise_refuses_a_generation_newer_than_the_control_plane() {
    let (broker, _dir) = broker_with_storage().await;
    let confirmed = ConfirmedAs::new(Confirmed::Assigned {
        generation: 5,
        leader: "broker-a".to_string(),
    });
    let on = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6))
        .with_fleet(marks(true))
        .with_confirmed_routes(Arc::clone(&confirmed) as _);
    let plain = Fence {
        election: None,
        ..fence_with(6, set(5, OWN), None, false)
    };
    let answer = on.fence(SENDER, plain.clone()).await;
    assert_eq!(refusal(&answer).code, ErrorCode::StaleRoute);

    let off = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6))
        .with_fleet(marks(false))
        .with_confirmed_routes(confirmed as _);
    let answer = off.fence(SENDER, plain).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
}

/// A routing view behind the fence's generation is usual for a fence, so
/// it does not refuse a copy with no promise: the confirmed read decides,
/// and here the control plane already confirms 6.
#[tokio::test]
async fn a_view_behind_the_fence_leaves_the_confirmed_read_to_decide() {
    let (broker, _dir) = broker_with_storage().await;
    let plain = Fence {
        election: None,
        ..fence_with(6, set(5, OWN), None, false)
    };
    let confirmed = ConfirmedAs::new(Confirmed::Assigned {
        generation: 6,
        leader: "broker-a".to_string(),
    });
    let on = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5))
        .with_fleet(marks(true))
        .with_confirmed_routes(Arc::clone(&confirmed) as _);
    let answer = on.fence(SENDER, plain).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
}

/// **Without `joint_fence` nothing changes**: a copy with no set answers a
/// fence and takes a batch, and no learner mark is written.
#[tokio::test]
async fn with_the_gate_off_a_copy_with_no_set_answers_as_before() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6))
        .with_fleet(marks(false))
        .with_local_storage(StorageId::from_bytes(OWN));
    let answer = handler
        .fence(SENDER, fence_with(6, set(5, OLD), None, true))
        .await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    let answer = handler
        .apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let log = log_of(&broker).await;
    assert_eq!(log.member_is_learner(LOCAL), Some(false));
    let answer = handler.pre_vote(pre_vote(7)).await;
    assert!(matches!(answer, InternalMessage::PreVoteAnswer(_)));
}
