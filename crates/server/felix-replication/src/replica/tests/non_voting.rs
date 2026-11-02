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
    Arc::new(build_marks(joint).allow_non_voting())
}

/// The gate as a registration's answer left it, on a build that does not
/// yet mark wiped copies.
fn build_marks(joint: bool) -> QuorumMarks {
    let fleet = FleetGate::new([JOINT_FENCE.name()]);
    fleet.restart(joint.then_some(JOINT_FENCE.name()));
    QuorumMarks::with_fleet(Arc::new(fleet))
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

/// **A re-admitted copy keeps the ballot its catch-up took.** Before the
/// wipe it may have promised any generation up to the one it caught up at,
/// to a candidate it no longer knows of. Holding that leader's ballot, it
/// promises none of them to anyone else once it votes again (`CatchUp` in
/// `docs/formal/FelixShard.tla`).
#[tokio::test]
async fn a_readmitted_copy_keeps_the_ballot_its_catch_up_took() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, OWN);
    wiped_at_5(&handler).await;
    let answer = handler.fence(SENDER, readmission(6, OWN)).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert_eq!(
        log.accepted_generation(),
        0,
        "a learner's refusal promises nothing"
    );

    let answer = handler
        .apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(log.member_is_learner(LOCAL), Some(false));
    assert_eq!(log.accepted_generation(), 6);
    assert_eq!(log.accepted_leader().as_deref(), Some("broker-a"));

    let answer = handler
        .fence(Some("broker-c"), fence_with(6, set(6, OWN), None, true))
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch, "{answer:?}");
    assert_eq!(log.accepted_leader().as_deref(), Some("broker-a"));
}

/// A leader at generation 7 shipping the copy the log from the start of 6, the
/// generation of the set that re-admitted it. Unstamped, so the copy keeps
/// that set: a stamp would swap it for the routing view's, which names no
/// storage ids, and the copy would wait for a set that names its own.
fn catch_up_to_7() -> ReplicateRecords {
    ReplicateRecords {
        set_generation: None,
        generations: Some(vec![
            GenerationStart {
                generation: 6,
                start_offset: 0,
            },
            GenerationStart {
                generation: 7,
                start_offset: 1,
            },
        ]),
        ..batch(7, 0, &["a", "b"])
    }
}

/// The same when the leader that catches the copy up is at a later
/// generation than the set the copy kept: the ballot it takes is the
/// leader's, so a fence at the copy's own generation is refused.
#[tokio::test]
async fn a_readmitted_copy_keeps_the_ballot_of_a_later_leader() {
    let (broker, _dir) = broker_with_storage().await;
    // The re-admission came while the control plane confirmed 6; the
    // catch-up comes after it moved on to 7.
    let at_6 = handler(&broker, 6, OWN);
    wiped_at_5(&at_6).await;
    let answer = at_6.fence(SENDER, readmission(6, OWN)).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert_eq!(log.set_generation(), 6);
    let handler = handler(&broker, 7, OWN);

    let answer = handler
        .apply(SENDER, catch_up_to_7(), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(log.member_is_learner(LOCAL), Some(false));
    assert_eq!(log.accepted_generation(), 7);
    assert_eq!(log.accepted_leader().as_deref(), Some("broker-a"));

    for candidate in [false, true] {
        let answer = handler
            .fence(
                Some("broker-c"),
                fence_with(6, set(6, OWN), None, candidate),
            )
            .await;
        assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch, "{answer:?}");
    }
    assert_eq!(log.accepted_generation(), 7);
    assert_eq!(log.accepted_leader().as_deref(), Some("broker-a"));
}

/// **A broker the control plane last registered under another storage id
/// is a learner for a shard it may have held.** The wipe came before any set
/// was heard, so the first set it learns names its new id: no evidence in the
/// set, only in the registration (`ReplacedIdLearner` in
/// `docs/formal/FelixShard.tla`). It refuses the fence, and is admitted once
/// the start record of that set's generation reaches its log.
#[tokio::test]
async fn a_replaced_storage_id_is_a_learner() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 2, OWN).with_replaced_storage(true);

    let answer = handler
        .fence(SENDER, fence_with(2, set(2, OWN), None, false))
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert_eq!(log.set_generation(), 2, "the set was not learned");
    assert_eq!(log.member_is_learner(LOCAL), Some(true));

    let answer = handler
        .apply(SENDER, stamped(2, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(log.member_is_learner(LOCAL), Some(false));
}

/// **A replaced copy refuses a fence that carries no set**, as a leader
/// sends one to a replica whose capabilities it has not learned. Answered,
/// it would count with an empty log toward a majority the acknowledged
/// records need (`SetlessFences` in `docs/formal/FelixShard.tla`).
#[tokio::test]
async fn a_replaced_copy_refuses_a_fence_without_a_set() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, OWN).with_replaced_storage(true);
    let plain = Fence {
        election: None,
        ..fence_with(6, named(6, None), None, false)
    };

    let answer = handler.fence(SENDER, plain).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized, "{answer:?}");
    assert_eq!(log_of(&broker).await.accepted_generation(), 0);
}

/// **A cache's counter fence asks the cache log whether this copy votes**:
/// the counter log keeps no set, so a learner's empty counter log would
/// otherwise count, and counter increments a majority acknowledged before
/// the wipe would be lost (`AnswerCounterFence` in
/// `docs/formal/FelixShard.tla`).
#[tokio::test]
async fn a_counter_fence_is_refused_while_the_cache_copy_is_a_learner() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handler = cache_handler(dir.path());
    let counters = on(
        ReplicaLog::Counters,
        Fence {
            election: None,
            ..fence_with(6, named(6, None), None, false)
        },
    );

    let answer = handler
        .fence(
            SENDER,
            on(ReplicaLog::Cache, fence_with(6, set(5, OLD), None, false)),
        )
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let answer = handler.fence(SENDER, counters).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized, "{answer:?}");
}

/// A handler for a cache shard with counters, as `handler` is for a stream.
fn cache_handler(dir: &std::path::Path) -> ReplicaHandler {
    let broker = super::fence::cache_broker(dir);
    ReplicaHandler::new(broker, super::fence::cache_router(6))
        .with_fleet(marks(true))
        .with_local_storage(StorageId::from_bytes(OWN))
        .with_stickiness(Duration::ZERO)
        .with_confirmed_routes(ConfirmedAs::new(Confirmed::Assigned {
            generation: 6,
            leader: "broker-a".to_string(),
        }) as _)
}

fn on(log: ReplicaLog, fence: Fence) -> Fence {
    Fence { log, ..fence }
}

/// **A cache copy is re-admitted only once its counter log holds the start
/// of the set's generation too.** Admitted on the cache log alone, it would
/// answer the next counter fence with a counter log that lacks updates a
/// majority acknowledged (`ReadmitChecksCounters` in
/// `docs/formal/FelixShard.tla`).
#[tokio::test]
async fn a_copy_is_readmitted_only_once_its_counter_log_caught_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handler = cache_handler(dir.path()).with_replaced_storage(true);
    let counters = on(
        ReplicaLog::Counters,
        Fence {
            election: None,
            ..fence_with(6, named(6, None), None, false)
        },
    );
    let answer = handler
        .fence(
            SENDER,
            on(ReplicaLog::Cache, fence_with(6, set(6, OWN), None, false)),
        )
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);

    let put = super::fence::cache_put("k", "v");
    let answer = handler
        .apply(SENDER, stamped(6, 0, &[&put]), felix_broker::LogKind::Cache)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let answer = handler.fence(SENDER, counters.clone()).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized, "{answer:?}");

    let unstamped = ReplicateRecords {
        set_generation: None,
        ..stamped(6, 0, &["d1"])
    };
    let answer = handler
        .apply(SENDER, unstamped, felix_broker::LogKind::Counters)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let answer = handler.fence(SENDER, counters).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
}

/// A shard's creation is a set at generation 1, which no copy held before:
/// a replaced storage id votes in it.
#[tokio::test]
async fn a_replaced_storage_id_votes_at_creation() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 1, OWN).with_replaced_storage(true);

    let answer = handler
        .fence(SENDER, fence_with(1, set(1, OWN), None, false))
        .await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    let log = log_of(&broker).await;
    assert_eq!(log.member_is_learner(LOCAL), Some(false));
}

/// **A replaced copy is a learner in a generation-1 set that names no ids.**
/// A shard created before sets named member ids keeps such a set until a
/// placement writes another, so the set says nothing of when the shard was
/// created: the copy may have held it under its old id. From a fence and
/// from a batch alike (`Forget` marks whatever the generation in
/// `docs/formal/FelixShard.tla`).
#[tokio::test]
async fn a_replaced_copy_is_a_learner_in_an_idless_creation_set() {
    let (broker, _dir) = broker_with_storage().await;
    let fenced = handler(&broker, 1, OWN).with_replaced_storage(true);
    let answer = fenced
        .fence(SENDER, fence_with(1, named(1, None), None, false))
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized, "{answer:?}");
    assert_eq!(log_of(&broker).await.member_is_learner(LOCAL), Some(true));

    let (broker, _dir) = broker_with_storage().await;
    let shipped = handler(&broker, 1, OWN).with_replaced_storage(true);
    let answer = shipped
        .apply(SENDER, stamped(1, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        !matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(log_of(&broker).await.member_is_learner(LOCAL), Some(true));
}

/// **A broker new to the control plane votes.** Its storage id is as new as
/// a wiped one's, but it held nothing under another id: a set past
/// generation 1 naming it, as a swap that adds it writes, is answered.
#[tokio::test]
async fn a_new_brokers_copy_votes_past_creation() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 2, OWN);

    let answer = handler
        .fence(SENDER, fence_with(2, set(2, OWN), None, false))
        .await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    let log = log_of(&broker).await;
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
        .with_fleet(Arc::new(
            QuorumMarks::with_fleet(Arc::clone(&fleet)).allow_non_voting(),
        ))
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

/// **A route's member ids alone make a wiped copy a learner.** The assignment
/// names no failover ids, only each member's registered storage id, as every
/// placement write does; a batch stamped with that set's generation teaches
/// the copy it, so a move or a creation no longer lets a wiped copy vote.
#[tokio::test]
async fn a_route_naming_member_ids_makes_a_wiped_copy_a_learner() {
    let (broker, _dir) = broker_with_storage().await;
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes: HashMap<String, NodeRef> = ["broker-a", LOCAL]
        .into_iter()
        .zip(7001..)
        .map(|(id, port)| (id.to_string(), node(id, port)))
        .collect();
    let table = RoutingTable::build_with(
        [felix_router::Placed {
            key: key(),
            leader: "broker-a".to_string(),
            replicas: vec![LOCAL.to_string()],
            generation: 6,
            draining: false,
            successor: None,
            routing: Default::default(),
            incarnation: 0,
            fence_storage_ids: Default::default(),
            member_storage_ids: [
                (
                    "broker-a".to_string(),
                    StorageId::from_bytes([1; 16]).to_hex(),
                ),
                (LOCAL.to_string(), StorageId::from_bytes(OLD).to_hex()),
            ]
            .into(),
            pending: None,
        }],
        &nodes,
    );
    router.publish(table, &nodes);
    let handler = ReplicaHandler::new(Arc::clone(&broker), router)
        .with_fleet(marks(true))
        .with_local_storage(StorageId::from_bytes(OWN))
        .with_stickiness(Duration::ZERO)
        .with_confirmed_routes(ConfirmedAs::new(Confirmed::Assigned {
            generation: 6,
            leader: "broker-a".to_string(),
        }) as _);

    let _ = handler
        .apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    let log = log_of(&broker).await;
    assert_eq!(log.set_generation(), 6, "the route's set was not learned");
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

    let unready = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6))
        .with_fleet(Arc::new(build_marks(true)))
        .with_confirmed_routes(Arc::clone(&confirmed) as _);
    let answer = unready.fence(SENDER, plain.clone()).await;
    assert!(
        matches!(answer, InternalMessage::FenceOk(_)),
        "a build that does not mark wiped copies answers as before: {answer:?}"
    );

    let (broker, _dir) = broker_with_storage().await;
    let off = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6))
        .with_fleet(marks(false))
        .with_confirmed_routes(confirmed as _);
    let answer = off.fence(SENDER, plain).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
}

/// The control plane answering what a test last set.
struct Moving(parking_lot::Mutex<Confirmed>);

#[async_trait::async_trait]
impl crate::confirmed::ConfirmedRoutes for Moving {
    async fn confirmed(&self, _key: &felix_router::ShardKey) -> Confirmed {
        self.0.lock().clone()
    }
}

/// **A build that does not mark wiped copies reads the control plane live**,
/// as it did before the confirmed cache: a leader deposed a moment ago is
/// refused, though a read at its generation named it.
#[tokio::test]
async fn before_the_build_allows_it_confirmed_reads_are_not_cached() {
    let (broker, _dir) = broker_with_storage().await;
    let confirmed = Arc::new(Moving(parking_lot::Mutex::new(Confirmed::Assigned {
        generation: 6,
        leader: "broker-a".to_string(),
    })));
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5))
        .with_fleet(Arc::new(build_marks(true)))
        .with_confirmed_routes(Arc::clone(&confirmed) as _);
    let plain = Fence {
        election: None,
        ..fence_with(6, set(5, OWN), None, false)
    };
    let answer = handler.fence(Some("broker-z"), plain.clone()).await;
    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch, "{answer:?}");

    *confirmed.0.lock() = Confirmed::Assigned {
        generation: 7,
        leader: "broker-c".to_string(),
    };
    let answer = handler.fence(SENDER, plain).await;
    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch, "{answer:?}");
    assert_eq!(log_of(&broker).await.accepted_generation(), 0);
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

/// **A copy re-admitted on its stream log answers no group fence until its
/// group logs hold a record of the re-admitting generation.** They carry no
/// start record, so the stream log's says nothing of them; empty, they would
/// count toward a majority that lacks a cursor commit a majority took.
#[tokio::test]
async fn a_readmitted_copy_answers_group_fences_once_its_group_logs_caught_up() {
    use super::groups::{group_broker, put, records};
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = group_broker(dir.path());
    let handler = handler(&broker, 6, OWN).with_replaced_storage(true);
    let answer = handler
        .fence(SENDER, fence_with(6, set(6, OWN), None, false))
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert!(log.member_is_wiped(LOCAL), "the marker's evidence marks it");

    let answer = handler
        .apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(log.member_is_learner(LOCAL), Some(false), "not re-admitted");
    assert!(log.member_is_wiped(LOCAL));
    let group_fence = |log| Fence {
        election: None,
        ..on(log, fence_with(6, named(6, None), None, false))
    };
    let answer = handler
        .fence(SENDER, group_fence(ReplicaLog::GroupCursors))
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized, "{answer:?}");

    let mut cursor = vec![2];
    for field in [2u64, 1, 4, 0] {
        cursor.extend_from_slice(&field.to_be_bytes());
    }
    for (kind, payload) in [
        (
            felix_broker::LogKind::GroupDeadLetters,
            put("workers\u{1f}1", b""),
        ),
        (felix_broker::LogKind::GroupCursors, put("workers", &cursor)),
    ] {
        let answer = handler.apply(SENDER, records(6, &[payload]), kind).await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "{answer:?}"
        );
    }
    assert!(!log.member_is_wiped(LOCAL), "settled");
    for kind in [ReplicaLog::GroupDeadLetters, ReplicaLog::GroupCursors] {
        let answer = handler.fence(SENDER, group_fence(kind)).await;
        assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    }
}

/// A move of this broker's copy to broker-d, at `generation`: a set that does
/// not list it.
fn moved(generation: u64) -> ReplicaSetView {
    ReplicaSetView {
        generation,
        members: [("broker-a", 1), ("broker-c", 2), ("broker-d", 4)]
            .into_iter()
            .map(|(node, id)| SetMember {
                node_id: node.to_string(),
                storage_id: Some([id; 16]),
                learner: node == "broker-d",
            })
            .collect(),
    }
}

/// **A replaced copy that first learns a set listing it only as leaving is
/// marked**, so a set naming it again later finds it a learner.
#[tokio::test]
async fn a_replaced_copy_listed_only_as_leaving_is_marked() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, OWN).with_replaced_storage(true);
    let answer = handler
        .fence(SENDER, fence_with(7, moved(7), Some(set(6, OWN)), false))
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert!(log.member_is_wiped(LOCAL));
    assert_eq!(log.member_is_learner(LOCAL), Some(true));

    let answer = handler
        .fence(SENDER, fence_with(8, set(8, OWN), None, false))
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized, "{answer:?}");
}

/// **A move away and back does not end the wiped mark.** The copy, re-admitted
/// on its stream log but not yet on its group logs, answers the move's fence
/// as a voter of the set being left, so it keeps a set that does not list it.
/// Named again later, it must still refuse group fences: its group logs are
/// as empty as before, and a leader counting one would open without a dead
/// letter a majority took.
#[tokio::test]
async fn a_set_that_moves_the_copy_away_and_back_keeps_it_wiped() {
    use super::groups::group_broker;
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = group_broker(dir.path());
    let handler = handler(&broker, 6, OWN).with_replaced_storage(true);
    let answer = handler
        .fence(SENDER, fence_with(6, set(6, OWN), None, false))
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let answer = handler
        .apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let log = log_of(&broker).await;
    assert_eq!(log.member_is_learner(LOCAL), Some(false), "re-admitted");
    assert!(log.member_is_wiped(LOCAL));

    // The move to broker-d: this copy is listed only in the set being left.
    let answer = handler
        .fence(SENDER, fence_with(7, moved(7), Some(set(6, OWN)), false))
        .await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    assert!(
        log.member_is_wiped(LOCAL),
        "a set listing it only as leaving keeps the mark"
    );

    // Placed back in the set.
    let answer = handler
        .fence(SENDER, fence_with(8, set(8, OWN), None, false))
        .await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    assert!(log.member_is_wiped(LOCAL));
    for kind in [ReplicaLog::GroupDeadLetters, ReplicaLog::GroupCursors] {
        let fence = Fence {
            election: None,
            ..on(kind, fence_with(8, named(8, None), None, false))
        };
        let answer = handler.fence(SENDER, fence).await;
        assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized, "{answer:?}");
    }
}

/// **A cache copy whose counter log cannot be opened is not re-admitted.**
/// Its counter log may lack updates a majority acknowledged, and nothing
/// says otherwise.
#[tokio::test]
async fn a_copy_whose_counter_log_cannot_open_stays_a_learner() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handler = cache_handler(dir.path()).with_replaced_storage(true);
    // Nothing can be created under the counter store's root now.
    std::fs::remove_dir_all(dir.path().join("counters")).expect("remove");
    std::fs::write(dir.path().join("counters"), b"").expect("block");
    let answer = handler
        .fence(
            SENDER,
            on(ReplicaLog::Cache, fence_with(6, set(6, OWN), None, false)),
        )
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);

    let put = super::fence::cache_put("k", "v");
    let answer = handler
        .apply(SENDER, stamped(6, 0, &[&put]), felix_broker::LogKind::Cache)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert!(
        handler
            .broker
            .shard_log(
                felix_broker::LogKind::Counters,
                TENANT,
                NAMESPACE,
                STREAM,
                0
            )
            .await
            .is_none(),
        "the counter log opened"
    );
    let cache = handler
        .broker
        .shard_log(felix_broker::LogKind::Cache, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("cache log");
    assert_eq!(cache.member_is_learner(LOCAL), Some(true), "admitted");
}

/// **A build that does not yet mark wiped copies leaves a fleet that
/// finalized `joint_fence` as it was**: a set naming another storage id for
/// this copy is kept as it came, and the fence is answered.
#[tokio::test]
async fn before_the_build_allows_it_a_wiped_copy_is_not_marked() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, OWN)
        .with_fleet(Arc::new(build_marks(true)))
        .with_replaced_storage(true);
    let answer = handler
        .fence(SENDER, fence_with(6, set(5, OLD), None, false))
        .await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    let log = log_of(&broker).await;
    assert_eq!(log.member_is_learner(LOCAL), Some(false));
    assert!(!log.member_is_wiped(LOCAL));
}
