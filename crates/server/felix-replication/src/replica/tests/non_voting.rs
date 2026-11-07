//! A copy that lost its storage votes in nothing until a set change
//! re-admits it (#1331): `NonVoter`, `Readmit`, `Readmitted` and `Creates` in
//! `docs/formal/FelixShard.tla`. Only a set naming another storage id for the
//! copy is evidence of the wipe.
use std::time::Duration;

use felix_common::fleet::{FleetGate, JOINT_FENCE};
use felix_storage::StorageId;
use felix_storage::disk_log::{Member, Membership, ReplicaSet};
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
            earlier: Vec::new(),
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
    let answer = settled(|| handler.fence(SENDER, fence_with(6, set(5, OLD), None, false))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
}

/// **A wiped copy refuses a fence and a candidate's vote**, takes no ballot,
/// and still learns the set the fence carried, as a learner.
#[tokio::test]
async fn a_wiped_copy_refuses_a_fence_and_a_vote() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, OWN);

    for candidate in [false, true] {
        let answer =
            settled(|| handler.fence(SENDER, fence_with(6, set(5, OLD), None, candidate))).await;
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
        earlier: Vec::new(),
    })
    .await
    .expect("set");
    assert_eq!(log.member_is_learner(LOCAL), Some(false));

    let answer = settled(|| handler.fence(SENDER, fence_with(6, set(5, OWN), None, true))).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
}

/// **A wiped copy answers a pre-vote** and writes nothing, but its answer
/// is a refusal no asker counts.
#[tokio::test]
async fn a_wiped_copy_answers_a_pre_vote_without_voting() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, OWN);
    wiped_at_5(&handler).await;

    let answer = settled(|| handler.pre_vote(pre_vote(7))).await;
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
    let answer = settled(|| handler.fence(SENDER, fence_with(6, set(5, OWN), None, true))).await;
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

    let answer = settled(|| handler.fence(SENDER, readmission(6, OWN))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert_eq!(log.accepted_generation(), 0);

    // Not even catching up admits it.
    let answer =
        settled(|| handler.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream))
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

    let answer = settled(|| handler.fence(SENDER, readmission(6, OWN))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert_eq!(log.set_generation(), 6, "the set was not learned");
    assert_eq!(log.member_is_learner(LOCAL), Some(true));

    let answer =
        settled(|| handler.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream))
            .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(log.member_is_learner(LOCAL), Some(false));
}

/// **A start record older than the set's generation re-admits nothing.**
/// Only the start of the generation that names this copy follows everything
/// the set being left held; an older one may not.
#[tokio::test]
async fn an_older_start_record_leaves_the_copy_a_learner() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, OWN);
    wiped_at_5(&handler).await;
    let answer = settled(|| handler.fence(SENDER, readmission(6, OWN))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;

    let older = ReplicateRecords {
        set_generation: Some(6),
        generations: Some(vec![GenerationStart {
            generation: 5,
            start_offset: 0,
        }]),
        ..batch(6, 0, &["a"])
    };
    let answer =
        settled(|| handler.apply(SENDER, older.clone(), felix_broker::LogKind::Stream)).await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(log.member_is_learner(LOCAL), Some(true));

    let answer =
        settled(|| handler.apply(SENDER, stamped(6, 1, &["b"]), felix_broker::LogKind::Stream))
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
    let answer = settled(|| handler.fence(SENDER, readmission(6, OWN))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert_eq!(
        log.accepted_generation(),
        0,
        "a learner's refusal promises nothing"
    );

    let answer =
        settled(|| handler.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream))
            .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(log.member_is_learner(LOCAL), Some(false));
    assert_eq!(log.accepted_generation(), 6);
    assert_eq!(log.accepted_leader().as_deref(), Some("broker-a"));

    let answer =
        settled(|| handler.fence(Some("broker-c"), fence_with(6, set(6, OWN), None, true))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch, "{answer:?}");
    assert_eq!(log.accepted_leader().as_deref(), Some("broker-a"));
}

/// A leader at generation 7 shipping the copy the log from the start of 6, the
/// generation of the set that re-admitted it. Unstamped, so the copy keeps
/// that set: a stamp would swap it for the routing view's, which names no
/// storage ids, and the copy would wait for a set that names its own.
fn catch_up_to_7() -> ReplicateRecords {
    ReplicateRecords {
        headers: None,
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
    let answer = settled(|| at_6.fence(SENDER, readmission(6, OWN))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert_eq!(log.set_generation(), 6);
    let handler = handler(&broker, 7, OWN);

    let answer =
        settled(|| handler.apply(SENDER, catch_up_to_7(), felix_broker::LogKind::Stream)).await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(log.member_is_learner(LOCAL), Some(false));
    assert_eq!(log.accepted_generation(), 7);
    assert_eq!(log.accepted_leader().as_deref(), Some("broker-a"));

    for candidate in [false, true] {
        let answer = settled(|| {
            handler.fence(
                Some("broker-c"),
                fence_with(6, set(6, OWN), None, candidate),
            )
        })
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

    let answer = settled(|| handler.fence(SENDER, fence_with(2, set(2, OWN), None, false))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert_eq!(log.set_generation(), 2, "the set was not learned");
    assert_eq!(log.member_is_learner(LOCAL), Some(true));

    let answer =
        settled(|| handler.apply(SENDER, stamped(2, 0, &["a"]), felix_broker::LogKind::Stream))
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

    let answer = settled(|| handler.fence(SENDER, plain.clone())).await;
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

    let answer = settled(|| {
        handler.fence(
            SENDER,
            on(ReplicaLog::Cache, fence_with(6, set(5, OLD), None, false)),
        )
    })
    .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let answer = settled(|| handler.fence(SENDER, counters.clone())).await;
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
    let answer = settled(|| {
        handler.fence(
            SENDER,
            on(ReplicaLog::Cache, fence_with(6, set(6, OWN), None, false)),
        )
    })
    .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);

    let put = super::fence::cache_put("k", "v");
    let answer =
        settled(|| handler.apply(SENDER, stamped(6, 0, &[&put]), felix_broker::LogKind::Cache))
            .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let answer = settled(|| handler.fence(SENDER, counters.clone())).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized, "{answer:?}");

    let unstamped = ReplicateRecords {
        headers: None,
        set_generation: None,
        ..stamped(6, 0, &["d1"])
    };
    let answer =
        settled(|| handler.apply(SENDER, unstamped.clone(), felix_broker::LogKind::Counters)).await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let answer = settled(|| handler.fence(SENDER, counters.clone())).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
}

/// **A learner says so in every answer until it is re-admitted**, so the
/// leader leaves it out of its marks (#1762). The cache log alone does not
/// re-admit the copy, so that answer is a learner's; the counter batch that
/// completes the catch-up is answered as a voter's.
#[tokio::test]
async fn a_learner_says_so_in_its_answers_until_it_is_readmitted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handler = cache_handler(dir.path()).with_replaced_storage(true);
    let answer = settled(|| {
        handler.fence(
            SENDER,
            on(ReplicaLog::Cache, fence_with(6, set(6, OWN), None, false)),
        )
    })
    .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);

    let put = super::fence::cache_put("k", "v");
    let answer =
        settled(|| handler.apply(SENDER, stamped(6, 0, &[&put]), felix_broker::LogKind::Cache))
            .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(ref ok) if ok.learner),
        "a learner's answer must say so: {answer:?}"
    );

    let unstamped = ReplicateRecords {
        set_generation: None,
        ..stamped(6, 0, &["d1"])
    };
    let answer =
        settled(|| handler.apply(SENDER, unstamped.clone(), felix_broker::LogKind::Counters)).await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(ref ok) if !ok.learner),
        "re-admitted, it answers as a voter: {answer:?}"
    );
}

/// **A copy that cannot read its own log answers as a learner.** Whether it
/// is one is judged on the shard's own log; unread, counting it as a voter
/// could count a wiped copy toward a mark.
#[tokio::test]
async fn a_copy_whose_own_log_cannot_open_answers_as_a_learner() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    // A cache with no log behind it: the counter log opens, the cache's does not.
    let broker = Arc::new(
        Broker::new(Box::new(felix_storage::EphemeralCache::new())).with_counters(Arc::new(
            felix_storage::CounterStore::open(dir.path().join("counters"), config)
                .expect("counters"),
        )),
    );
    let handler = ReplicaHandler::new(broker, super::fence::cache_router(6))
        .with_fleet(marks(true))
        .with_local_storage(StorageId::from_bytes(OWN))
        .with_stickiness(Duration::ZERO)
        .with_confirmed_routes(ConfirmedAs::new(Confirmed::Assigned {
            generation: 6,
            leader: "broker-a".to_string(),
        }) as _);
    let batch = ReplicateRecords {
        set_generation: None,
        ..stamped(6, 0, &["d1"])
    };
    let answer =
        settled(|| handler.apply(SENDER, batch.clone(), felix_broker::LogKind::Counters)).await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(ref ok) if ok.learner),
        "an unread own log must not answer as a voter: {answer:?}"
    );
}

/// A shard's creation is a set at generation 1, which no copy held before:
/// a replaced storage id votes in it.
#[tokio::test]
async fn a_replaced_storage_id_votes_at_creation() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 1, OWN).with_replaced_storage(true);

    let answer = settled(|| handler.fence(SENDER, fence_with(1, set(1, OWN), None, false))).await;
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
    let answer = settled(|| fenced.fence(SENDER, fence_with(1, named(1, None), None, false))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized, "{answer:?}");
    assert_eq!(log_of(&broker).await.member_is_learner(LOCAL), Some(true));

    let (broker, _dir) = broker_with_storage().await;
    let shipped = handler(&broker, 1, OWN).with_replaced_storage(true);
    let answer =
        settled(|| shipped.apply(SENDER, stamped(1, 0, &["a"]), felix_broker::LogKind::Stream))
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

    let answer = settled(|| handler.fence(SENDER, fence_with(2, set(2, OWN), None, false))).await;
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
    let answer = settled(|| at_6.fence(SENDER, readmission(6, OWN))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);

    let handler = handler(&broker, 7, OWN);
    let answer = settled(|| handler.fence(SENDER, fence_with(7, set(7, OWN), None, false))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert_eq!(log.set_generation(), 7);

    let answer =
        settled(|| handler.apply(SENDER, stamped(7, 0, &["a"]), felix_broker::LogKind::Stream))
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

    let answer =
        settled(|| handler.fence(SENDER, fence_with(1, named(1, None), None, false))).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    let log = log_of(&broker).await;
    assert_eq!(log.member_is_learner(LOCAL), Some(false));
    let answer =
        settled(|| handler.apply(SENDER, stamped(1, 0, &["a"]), felix_broker::LogKind::Stream))
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
    let answer =
        settled(|| before.apply(SENDER, batch(5, 0, &["a"]), felix_broker::LogKind::Stream)).await;
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

    let answer = settled(|| handler.fence(SENDER, fence_with(6, set(5, OLD), None, false))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unavailable);
    // A caller that counts by attestation waits for the gate too.
    let answer = settled(|| unified(&handler, fence_with(6, set(5, OLD), None, false))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unavailable);
    let log = log_of(&broker).await;
    assert_eq!(log.set_generation(), 0, "wrote before the gate was read");
    assert_eq!(log.accepted_generation(), 0);

    let answer =
        settled(|| handler.fence(SENDER, fence_with(6, named(5, None), None, false))).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");

    fleet.restart([JOINT_FENCE.name()]);
    let answer = settled(|| handler.fence(SENDER, fence_with(7, set(6, OLD), None, false))).await;
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
            readmitted: Default::default(),
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

    let _ = settled(|| handler.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream))
        .await;
    let log = log_of(&broker).await;
    assert_eq!(log.set_generation(), 6, "the route's set was not learned");
    assert_eq!(log.member_is_learner(LOCAL), Some(true));
}

/// A learner reuses a confirmed read for a run of batches at one
/// generation rather than waiting on a read for each.
#[tokio::test]
async fn a_learner_reuses_a_confirmed_read() {
    let (broker, _dir) = broker_with_storage().await;
    let reads = Arc::new(CountingConfirmed::default());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6))
        .with_fleet(marks(true))
        .with_local_storage(StorageId::from_bytes([9; 16]))
        .with_confirmed_routes(Arc::clone(&reads) as _);
    wiped_at_5(&handler).await;

    let answer = settled(|| {
        handler.apply(
            SENDER,
            stamped(6, 0, &["a", "a"]),
            felix_broker::LogKind::Stream,
        )
    })
    .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    // Answered from the read the first batch waited for, with no refusal.
    let answer = handler
        .apply(
            SENDER,
            stamped(6, 2, &["b", "b"]),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
}

/// The control plane confirming broker-a at 6, counting the reads.
#[derive(Default)]
struct CountingConfirmed(std::sync::atomic::AtomicUsize);

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
    let answer =
        settled(|| lagging.apply(SENDER, stamped(7, 0, &["a"]), felix_broker::LogKind::Stream))
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
    let answer = settled(|| on.fence(SENDER, plain.clone())).await;
    assert_eq!(refusal(&answer).code, ErrorCode::StaleRoute);

    let (broker, _dir) = broker_with_storage().await;
    let off = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6))
        .with_fleet(marks(false))
        .with_confirmed_routes(confirmed as _);
    let answer = settled(|| off.fence(SENDER, plain.clone())).await;
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

/// **Without joint fences a copy reuses a read taken since the restart**
/// (#1791). The read was taken after any wipe, so it is no older
/// than the promises the wipe cost. A leader deposed after the read can then
/// be taken, as by any copy its successor has not fenced yet; once the
/// successor's fence lands, the ballot refuses it.
#[tokio::test]
async fn without_joint_fences_a_read_since_the_restart_is_reused() {
    let (broker, _dir) = broker_with_storage().await;
    let confirmed = Arc::new(Moving(parking_lot::Mutex::new(Confirmed::Assigned {
        generation: 6,
        leader: "broker-a".to_string(),
    })));
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5))
        .with_fleet(marks(false))
        .with_confirmed_routes(Arc::clone(&confirmed) as _);
    let at = |generation| Fence {
        election: None,
        ..fence_with(generation, set(5, OWN), None, false)
    };
    let answer = settled(|| handler.fence(Some("broker-z"), at(6))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch, "{answer:?}");

    *confirmed.0.lock() = Confirmed::Assigned {
        generation: 7,
        leader: "broker-c".to_string(),
    };
    let answer = settled(|| handler.fence(SENDER, at(6))).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    let answer = settled(|| handler.fence(Some("broker-c"), at(7))).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    let answer = settled(|| handler.fence(SENDER, at(6))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch, "{answer:?}");
    assert_eq!(log_of(&broker).await.accepted_generation(), 7);
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
    let answer = settled(|| on.fence(SENDER, plain.clone())).await;
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
    let answer = settled(|| handler.fence(SENDER, fence_with(6, set(5, OLD), None, true))).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    let answer =
        settled(|| handler.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream))
            .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let log = log_of(&broker).await;
    assert_eq!(log.member_is_learner(LOCAL), Some(false));
    let answer = settled(|| handler.pre_vote(pre_vote(7))).await;
    assert!(matches!(answer, InternalMessage::PreVoteAnswer(_)));
}

/// **Without `joint_fence` a wiped copy is not marked** either: a set naming
/// another storage id for it is kept as it came, and the fence is answered.
#[tokio::test]
async fn with_the_gate_off_a_wiped_copy_is_not_marked() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, OWN)
        .with_fleet(marks(false))
        .with_replaced_storage(true);
    let answer = settled(|| handler.fence(SENDER, fence_with(6, set(5, OLD), None, false))).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    let log = log_of(&broker).await;
    assert_eq!(log.member_is_learner(LOCAL), Some(false));
    assert!(!log.member_is_wiped(LOCAL));
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
    let answer = settled(|| handler.fence(SENDER, fence_with(6, set(6, OWN), None, false))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert!(log.member_is_wiped(LOCAL), "the marker's evidence marks it");

    let answer =
        settled(|| handler.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream))
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
    let answer = settled(|| handler.fence(SENDER, group_fence(ReplicaLog::GroupCursors))).await;
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
        let answer =
            settled(|| handler.apply(SENDER, records(6, std::slice::from_ref(&payload)), kind))
                .await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "{answer:?}"
        );
    }
    assert!(!log.member_is_wiped(LOCAL), "settled");
    for kind in [ReplicaLog::GroupDeadLetters, ReplicaLog::GroupCursors] {
        let answer = settled(|| handler.fence(SENDER, group_fence(kind))).await;
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
    let answer =
        settled(|| handler.fence(SENDER, fence_with(7, moved(7), Some(set(6, OWN)), false))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let log = log_of(&broker).await;
    assert!(log.member_is_wiped(LOCAL));
    assert_eq!(log.member_is_learner(LOCAL), Some(true));

    let answer = settled(|| handler.fence(SENDER, fence_with(8, set(8, OWN), None, false))).await;
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
    let answer = settled(|| handler.fence(SENDER, fence_with(6, set(6, OWN), None, false))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let answer =
        settled(|| handler.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream))
            .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let log = log_of(&broker).await;
    assert_eq!(log.member_is_learner(LOCAL), Some(false), "re-admitted");
    assert!(log.member_is_wiped(LOCAL));

    // The move to broker-d: this copy is listed only in the set being left.
    let answer =
        settled(|| handler.fence(SENDER, fence_with(7, moved(7), Some(set(6, OWN)), false))).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    assert!(
        log.member_is_wiped(LOCAL),
        "a set listing it only as leaving keeps the mark"
    );

    // Placed back in the set.
    let answer = settled(|| handler.fence(SENDER, fence_with(8, set(8, OWN), None, false))).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    assert!(log.member_is_wiped(LOCAL));
    for kind in [ReplicaLog::GroupDeadLetters, ReplicaLog::GroupCursors] {
        let fence = Fence {
            election: None,
            ..on(kind, fence_with(8, named(8, None), None, false))
        };
        let answer = settled(|| handler.fence(SENDER, fence.clone())).await;
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
    let answer = settled(|| {
        handler.fence(
            SENDER,
            on(ReplicaLog::Cache, fence_with(6, set(6, OWN), None, false)),
        )
    })
    .await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);

    let put = super::fence::cache_put("k", "v");
    let answer =
        settled(|| handler.apply(SENDER, stamped(6, 0, &[&put]), felix_broker::LogKind::Cache))
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

/// **A promotion whose set being left still lists the copy as a learner does
/// not make it one again.** Re-admitted on its stream log but still wiped
/// (its group logs lag), the copy is promoted to a voter; the set it leaves
/// is the old leader's view from before the re-admission. Taking that as the
/// copy's own state would have it refuse the next leader's fence too, and
/// with one more failure no majority could form.
#[tokio::test]
async fn a_stale_learner_flag_in_the_set_being_left_does_not_demote_the_copy() {
    use super::groups::group_broker;
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = group_broker(dir.path());
    let handler = handler(&broker, 6, OWN).with_replaced_storage(true);
    let staged = |generation| {
        let mut set = set(generation, OWN);
        set.members[1].learner = true;
        set
    };
    let answer = settled(|| handler.fence(SENDER, fence_with(6, staged(6), None, false))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let answer =
        settled(|| handler.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream))
            .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let log = log_of(&broker).await;
    assert_eq!(log.member_is_learner(LOCAL), Some(false), "re-admitted");
    assert!(log.member_is_wiped(LOCAL));

    let answer =
        settled(|| handler.fence(SENDER, fence_with(7, set(7, OWN), Some(staged(6)), false))).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    assert_eq!(
        log.member_is_learner(LOCAL),
        Some(false),
        "the set being left is not this copy's state"
    );
    let answer =
        settled(|| handler.fence(SENDER, fence_with(8, set(8, OWN), Some(set(7, OWN)), true)))
            .await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
}

/// What a learner's batch costs, the one copy with no promise that takes
/// batches, for the pull request's numbers: `cargo test -p felix-replication
/// --release --lib learner_batch_cost -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "a timing report, not a check"]
async fn learner_batch_cost() {
    const BATCHES: u64 = 2_000;
    let (broker, _dir) = broker_with_storage().await;
    let reads = Arc::new(CountingConfirmed::default());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6))
        .with_fleet(marks(true))
        .with_local_storage(StorageId::from_bytes([9; 16]))
        .with_confirmed_routes(Arc::clone(&reads) as _);
    wiped_at_5(&handler).await;
    let answer =
        settled(|| handler.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream))
            .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let started = std::time::Instant::now();
    for batch_no in 1..=BATCHES {
        let answer = handler
            .apply(
                SENDER,
                stamped(6, batch_no, &["a"]),
                felix_broker::LogKind::Stream,
            )
            .await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "{answer:?}"
        );
    }
    let apply = started.elapsed() / BATCHES as u32;
    eprintln!("learner batch apply (no promise, 1 record) {apply:?}");
}

fn kept_set(generation: u64, nodes: &[&str]) -> Membership {
    Membership {
        generation,
        members: nodes
            .iter()
            .map(|node| Member {
                node: (*node).to_string(),
                storage: None,
                learner: false,
                wiped: false,
            })
            .collect(),
    }
}

/// **A batch that raises the set keeps the sets of unopened fences** until
/// its records are on disk. One whose records never land (here a gap) must
/// not leave the log with neither those records nor the sets a failover
/// from it needs to fence.
#[tokio::test]
async fn a_batch_that_is_not_stored_keeps_the_earlier_sets() {
    let (broker, _dir) = broker_with_storage().await;
    let fleet = felix_common::fleet::FleetGate::new([felix_common::fleet::JOINT_FENCE.name()]);
    fleet.restart([felix_common::fleet::JOINT_FENCE.name()]);
    // The control plane's set before generation 5 is what the batch's set
    // leaves; without it the batch is refused before it raises the set.
    let router = super::election::router_after(5, Some(&["broker-a", LOCAL, "broker-c"]));
    let handler = ReplicaHandler::new(Arc::clone(&broker), router).with_fleet(Arc::new(
        crate::quorum::QuorumMarks::with_fleet(Arc::new(fleet)),
    ));
    let unopened = kept_set(4, &[LOCAL, "broker-x", "broker-y"]);
    let left = kept_set(3, &[LOCAL, "broker-c", "broker-d"]);
    let log = log_of(&broker).await;
    log.accept_generation(4, Some("broker-x"))
        .await
        .expect("ballot");
    log.accept_set_keeping_earlier(ReplicaSet {
        current: unopened.clone(),
        leaving: Some(left.clone()),
        earlier: Vec::new(),
    })
    .await
    .expect("set");

    let answer = handler
        .apply(SENDER, stamped(5, 3, &["a"]), felix_broker::LogKind::Stream)
        .await;

    assert_eq!(refusal(&answer).code, ErrorCode::LogGap, "{answer:?}");
    let set = log_of(&broker).await.replica_set().expect("set");
    assert_eq!(set.current.generation, 5, "the batch raised the set");
    assert_eq!(set.earlier, vec![unopened, left]);
}

/// A learner marked on evidence of a lost copy, whose ballot is `ballot`,
/// answering the fence of broker-a moving on to generation 6. With
/// `kept_promises` it restarted after an unclean crash that kept every ballot;
/// without, its directory was wiped or restored.
async fn continuing_fence(
    ballot: Option<(u64, &str)>,
    kept_promises: bool,
) -> (InternalMessage, felix_broker::StreamLog) {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, OWN).with_unclean_start(Arc::new(
        crate::unclean::UncleanStart::new(LOCAL).with_kept_promises(kept_promises),
    ));
    wiped_at_5(&handler).await;
    let log = log_of(&broker).await;
    if let Some((generation, leader)) = ballot {
        log.accept_generation(generation, Some(leader))
            .await
            .expect("ballot");
    }
    let answer = settled(|| handler.fence(SENDER, readmission(6, OWN))).await;
    (answer, log)
}

/// **A doubted copy whose ballot names the leader at the generation before
/// promises that leader its next one.** Its storage id rotated after an
/// unclean crash, so it is a learner until a leader vouches for it, but the
/// crash kept its ballot: it promised broker-a generation 5, and broker-a,
/// moving on to 6, holds every record it might have lost. It promises, says
/// its ballot was kept, and stays marked: the promise is no vote for what it
/// holds (#2047).
#[tokio::test]
async fn a_learner_whose_ballot_names_the_leader_promises_its_next_generation() {
    let (answer, log) = continuing_fence(Some((5, "broker-a")), true).await;

    assert!(
        matches!(answer, InternalMessage::FenceOk(ok) if ok.kept_ballot),
        "{answer:?}"
    );
    assert_eq!(log.accepted_generation(), 6);
    assert_eq!(log.accepted_leader().as_deref(), Some("broker-a"));
    assert!(
        log.member_is_wiped(LOCAL),
        "the promise vouched for the copy"
    );
}

/// **A learner's ballot taken while its promises counted for nothing stays
/// no promise after a crash that keeps it.** A unified fence hands a learner
/// a ballot, and once the broker restarts that ballot looks like the kept one
/// above: an older caller continuing from it would count the copy, which may
/// have promised the same generation to another leader before its wipe. The
/// directory's copy-trust record keeps it unkept until readmission, for the
/// older caller's refusal and the attestation alike.
#[tokio::test]
async fn a_ballot_taken_as_a_learner_is_no_promise_after_a_crash() {
    let (broker, _dir) = broker_with_storage().await;
    let trust_dir = tempfile::tempdir().expect("tempdir");
    let on = |unclean: Option<Arc<crate::unclean::UncleanStart>>| {
        let marks = marks(true);
        marks.set_kept_ballots(true);
        marks.set_copy_trust(Arc::new(
            felix_storage::run_state::copy_trust::CopyTrust::open(trust_dir.path()).expect("open"),
        ));
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6))
            .with_fleet(marks)
            .with_local_storage(StorageId::from_bytes(OWN))
            .with_stickiness(Duration::ZERO)
            .with_confirmed_routes(ConfirmedAs::new(Confirmed::Assigned {
                generation: 6,
                leader: "broker-a".to_string(),
            }) as _);
        match unclean {
            Some(unclean) => handler.with_unclean_start(unclean),
            None => handler,
        }
    };

    // As a unified fence from broker-a at 5 would leave the learner.
    let learner = on(None);
    wiped_at_5(&learner).await;
    learner.unkeep(&key(), 5).await.expect("unkeep");
    let log = log_of(&broker).await;
    log.accept_generation(5, Some("broker-a"))
        .await
        .expect("ballot");
    drop(learner);

    // An unclean crash that kept every ballot.
    let restarted = on(Some(Arc::new(
        crate::unclean::UncleanStart::new(LOCAL).with_kept_promises(true),
    )));
    let answer = settled(|| restarted.fence(SENDER, readmission(6, OWN))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    assert_ne!(log.accepted_generation(), 6);
    assert_eq!(
        restarted.attestation(&key(), true, false, false).kept_from,
        u64::MAX
    );
}

/// **A learner that took a unified fence's ballot does not count for an
/// older caller after a crash that keeps it**: the answer to the unified
/// fence recorded the ballot as no promise before writing it.
#[tokio::test]
async fn a_learners_unified_ballot_does_not_count_after_a_crash() {
    let (broker, _dir) = broker_with_storage().await;
    let trust_dir = tempfile::tempdir().expect("tempdir");
    let on = |unclean: Option<Arc<crate::unclean::UncleanStart>>| {
        let marks = kept_marks();
        marks.set_copy_trust(Arc::new(
            felix_storage::run_state::copy_trust::CopyTrust::open(trust_dir.path()).expect("open"),
        ));
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6))
            .with_fleet(marks)
            .with_local_storage(StorageId::from_bytes(OWN))
            .with_stickiness(Duration::ZERO)
            .with_confirmed_routes(ConfirmedAs::new(Confirmed::Assigned {
                generation: 6,
                leader: "broker-a".to_string(),
            }) as _);
        match unclean {
            Some(unclean) => handler.with_unclean_start(unclean),
            None => handler,
        }
    };

    let learner = on(None);
    wiped_at_5(&learner).await;
    let answer = settled(|| unified(&learner, fence_with(6, set(5, OLD), None, false))).await;
    let InternalMessage::FenceOk(ok) = answer else {
        panic!("expected the fence to be taken, got {answer:?}");
    };
    assert_eq!(ok.attestation, no_promise());
    let log = log_of(&broker).await;
    assert_eq!(log.accepted_leader().as_deref(), Some("broker-a"));
    drop(learner);

    // An unclean crash that kept every ballot.
    let restarted = on(Some(Arc::new(
        crate::unclean::UncleanStart::new(LOCAL).with_kept_promises(true),
    )));
    let answer = settled(|| restarted.fence(SENDER, readmission(7, OWN))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized, "{answer:?}");
    assert_eq!(log.accepted_generation(), 6);
}

/// [`handler`], with the copy-trust record under `trust_dir`.
fn handler_with_trust(
    broker: &Arc<Broker>,
    generation: u64,
    trust_dir: &std::path::Path,
) -> (
    ReplicaHandler,
    Arc<felix_storage::run_state::copy_trust::CopyTrust>,
) {
    let trust =
        Arc::new(felix_storage::run_state::copy_trust::CopyTrust::open(trust_dir).expect("open"));
    let marks = marks(true);
    marks.set_kept_ballots(false);
    handler_with_trust_on(broker, generation, trust, marks)
}

fn handler_with_trust_on(
    broker: &Arc<Broker>,
    generation: u64,
    trust: Arc<felix_storage::run_state::copy_trust::CopyTrust>,
    marks: Arc<QuorumMarks>,
) -> (
    ReplicaHandler,
    Arc<felix_storage::run_state::copy_trust::CopyTrust>,
) {
    marks.set_copy_trust(Arc::clone(&trust));
    let handler = ReplicaHandler::new(Arc::clone(broker), router_with(&[LOCAL], generation))
        .with_fleet(marks)
        .with_local_storage(StorageId::from_bytes(OWN))
        .with_stickiness(Duration::ZERO)
        .with_confirmed_routes(ConfirmedAs::new(Confirmed::Assigned {
            generation,
            leader: "broker-a".to_string(),
        }) as _);
    (handler, trust)
}

/// **Only a readmission into a set newer than its unkept ballot makes a
/// copy's promises count again.** A batch to a copy that holds no set, or a
/// readmission at the generation it took that ballot at, does not: a wiped
/// copy's promises count for nothing until a set introduced after the loss
/// readmits it (the design's "Wipe" row).
#[tokio::test]
async fn only_a_newer_sets_readmission_ends_an_unkept_ballot() {
    let name = crate::unclean::copy_key(&crate::driver::watch_key(&key()));

    // No set at all: a batch is no readmission.
    let (broker, _dir) = broker_with_storage().await;
    let trust_dir = tempfile::tempdir().expect("tempdir");
    let (copy, trust) = handler_with_trust(&broker, 6, trust_dir.path());
    copy.unkeep(&key(), 5).await.expect("unkeep");
    let unstamped = || ReplicateRecords {
        set_generation: None,
        ..stamped(6, 0, &["a"])
    };
    let answer = settled(|| copy.apply(SENDER, unstamped(), felix_broker::LogKind::Stream)).await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(
        trust.unkept(&name),
        Some(felix_storage::run_state::copy_trust::Since::At(5)),
        "a batch alone"
    );

    // Readmitted into the set of 6.
    for (since, cleared) in [(6, false), (5, true)] {
        let (broker, _dir) = broker_with_storage().await;
        let trust_dir = tempfile::tempdir().expect("tempdir");
        let (copy, trust) = handler_with_trust(&broker, 6, trust_dir.path());
        wiped_at_5(&copy).await;
        copy.unkeep(&key(), since).await.expect("unkeep");
        let answer = settled(|| copy.fence(SENDER, readmission(6, OWN))).await;
        assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
        let answer =
            settled(|| copy.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream))
                .await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "{answer:?}"
        );
        assert_eq!(log_of(&broker).await.member_is_learner(LOCAL), Some(false));
        assert_eq!(
            trust.unkept(&name).is_none(),
            cleared,
            "unkept since {since}"
        );
    }
}

/// **A readmission waits for the start to say what it kept.** A start from
/// the control-plane view says nothing until its restore check passes; a
/// batch before then must leave the copy's floor for the readmission after.
#[tokio::test]
async fn a_readmission_waits_until_the_start_says_what_it_kept() {
    let name = crate::unclean::copy_key(&crate::driver::watch_key(&key()));
    let (broker, _dir) = broker_with_storage().await;
    let trust_dir = tempfile::tempdir().expect("tempdir");
    felix_storage::storage_identity::replace(trust_dir.path(), OWN).expect("storage id");
    let trust = Arc::new(
        felix_storage::run_state::copy_trust::CopyTrust::open(trust_dir.path()).expect("open"),
    );
    let marks = marks(true);
    let (copy, trust) = handler_with_trust_on(&broker, 6, trust, Arc::clone(&marks));
    wiped_at_5(&copy).await;
    copy.unkeep(&key(), 5).await.expect("unkeep");
    let answer = settled(|| copy.fence(SENDER, readmission(6, OWN))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let first = || copy.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream);
    let answer = settled(first).await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(
        trust.unkept(&name),
        Some(felix_storage::run_state::copy_trust::Since::At(5)),
        "before the start says what it kept"
    );

    marks.set_readmits(true);
    marks.set_kept_ballots(false);
    let answer = settled(|| {
        copy.apply(
            SENDER,
            ReplicateRecords {
                set_generation: Some(6),
                ..batch(6, 1, &["b"])
            },
            felix_broker::LogKind::Stream,
        )
    })
    .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert!(trust.unkept(&name).is_none(), "cleared once it has");
    assert!(trust.readmitted(&name).is_some(), "readmitted once it has");
}

/// **A copy unkept since a generation the record lost is unkept since at
/// least the ballot it holds.** The record did not decode; the copy holds
/// broker-a's ballot at 6, maybe an unkept one, so a readmission into the set
/// of 6 does not end it.
#[tokio::test]
async fn an_unknown_unkept_generation_is_floored_at_the_ballot() {
    use felix_storage::run_state::copy_trust::Since;
    let name = crate::unclean::copy_key(&crate::driver::watch_key(&key()));
    let (broker, _dir) = broker_with_storage().await;
    let trust_dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        trust_dir
            .path()
            .join(felix_storage::run_state::copy_trust::FILE_NAME),
        b"junk",
    )
    .expect("write");
    let (copy, trust) = handler_with_trust(&broker, 6, trust_dir.path());
    wiped_at_5(&copy).await;
    log_of(&broker)
        .await
        .accept_generation(6, Some("broker-a"))
        .await
        .expect("ballot");
    assert_eq!(trust.unkept(&name), Some(Since::Unknown));

    let answer = settled(|| copy.fence(SENDER, readmission(6, OWN))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let answer =
        settled(|| copy.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream)).await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(log_of(&broker).await.member_is_learner(LOCAL), Some(false));
    assert_eq!(trust.unkept(&name), Some(Since::At(6)));
}

/// **A copy whose ballot did not survive still refuses.** Wiped, it names no
/// leader; one whose ballot names another leader, or broker-a at an older
/// generation, may have promised generation 6 to someone else. So may a
/// wiped or restored copy whose ballot names broker-a at 5: it may have taken
/// that ballot after losing a promise of 6 to a candidate, and its promise
/// now could make a second leader there (#1331).
#[tokio::test]
async fn a_learner_without_the_leaders_ballot_still_refuses_its_next_generation() {
    for (ballot, kept) in [
        (None, true),
        (Some((5, "broker-c")), true),
        (Some((4, "broker-a")), true),
        (Some((5, "broker-a")), false),
    ] {
        let (answer, log) = continuing_fence(ballot, kept).await;

        assert_eq!(
            refusal(&answer).code,
            ErrorCode::Unauthorized,
            "{ballot:?} {kept}"
        );
        assert_ne!(log.accepted_generation(), 6, "{ballot:?} {kept}");
    }
}

/// Marks under `joint_fence` for a directory that kept every ballot.
fn kept_marks() -> Arc<QuorumMarks> {
    let marks = marks(true);
    marks.set_kept_ballots(true);
    marks
}

fn unified(
    handler: &ReplicaHandler,
    fence: Fence,
) -> impl std::future::Future<Output = InternalMessage> + '_ {
    handler.fence_from(SENDER, PeerCapabilities::UNIFIED_FENCE, fence)
}

fn no_promise() -> Option<felix_wire::internal::FenceAttestation> {
    Some(felix_wire::internal::FenceAttestation {
        kept_from: u64::MAX,
        trusted: false,
        by_record: false,
    })
}

/// **To a caller that counts by attestation a wiped copy answers, and its
/// promise counts for nothing.** It takes the ballot, which only refuses
/// more, and stays a learner. Without the bit it refuses as before. The set
/// check still refuses an older set, and the ballot an older generation.
#[tokio::test]
async fn a_wiped_copy_answers_a_unified_fence_with_no_promise() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, OWN).with_fleet(kept_marks());
    for candidate in [false, true] {
        let answer =
            settled(|| handler.fence(SENDER, fence_with(6, set(5, OLD), None, candidate))).await;
        assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    }

    let answer = settled(|| unified(&handler, fence_with(6, set(5, OLD), None, false))).await;
    let InternalMessage::FenceOk(ok) = answer else {
        panic!("expected the fence to be taken, got {answer:?}");
    };
    assert_eq!(ok.attestation, no_promise());
    let log = log_of(&broker).await;
    assert_eq!(log.accepted_generation(), 6);
    assert_eq!(log.member_is_learner(LOCAL), Some(true));

    let answer = settled(|| unified(&handler, fence_with(6, set(4, OLD), None, false))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::StaleSet);
    let answer = settled(|| unified(&handler, fence_with(5, set(5, OLD), None, false))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch);
}

/// **A copy the fence's set lists only as a learner answers a unified
/// fence with no promise**, where it refuses an older caller as no voter in
/// that set. A broker the set does not list refuses either way.
#[tokio::test]
async fn a_copy_listed_as_a_learner_answers_a_unified_fence_with_no_promise() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = handler(&broker, 6, OWN).with_fleet(kept_marks());
    let mut listed = set(5, OWN);
    for member in &mut listed.members {
        member.learner = member.node_id == LOCAL;
    }
    let answer =
        settled(|| handler.fence(SENDER, fence_with(6, listed.clone(), None, false))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);

    let answer = settled(|| unified(&handler, fence_with(6, listed.clone(), None, false))).await;
    let InternalMessage::FenceOk(ok) = answer else {
        panic!("expected the fence to be taken, got {answer:?}");
    };
    assert_eq!(ok.attestation, no_promise());

    listed.members.retain(|member| member.node_id != LOCAL);
    let answer = settled(|| unified(&handler, fence_with(6, listed.clone(), None, false))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
}

/// **A learner continuing from its leader keeps no promise to a unified
/// caller**, though it promises an older one
/// (`a_learner_whose_ballot_names_the_leader_promises_its_next_generation`).
/// A unified fence hands a
/// learner a ballot, so the ballot it holds may be one it took while its
/// promises counted for nothing: a crash that kept that ballot does not make
/// it a kept promise.
#[tokio::test]
async fn a_continuing_learner_keeps_no_promise_to_a_unified_fence() {
    for ballot in ["broker-a", "broker-c"] {
        let (broker, _dir) = broker_with_storage().await;
        let handler = handler(&broker, 6, OWN)
            .with_fleet(kept_marks())
            .with_unclean_start(Arc::new(
                crate::unclean::UncleanStart::new(LOCAL).with_kept_promises(true),
            ));
        wiped_at_5(&handler).await;
        log_of(&broker)
            .await
            .accept_generation(5, Some(ballot))
            .await
            .expect("ballot");

        let answer = settled(|| unified(&handler, readmission(6, OWN))).await;
        let InternalMessage::FenceOk(ok) = answer else {
            panic!("expected the fence to be taken, got {answer:?}");
        };
        assert_eq!(ok.attestation, no_promise(), "{ballot}");
    }
}

/// **A unified answer that keeps no promise routes nothing to the
/// candidate**, as the refusal an older caller gets routes nothing. A
/// voter's answer still does.
#[tokio::test]
async fn a_learners_unified_answer_does_not_record_the_candidate() {
    let key = felix_router::ShardKey {
        tenant_id: TENANT.into(),
        namespace: NAMESPACE.into(),
        stream: STREAM.into(),
        shard: 0,
        kind: felix_router::ShardKind::Stream,
    };
    for learner in [true, false] {
        let (broker, _dir) = broker_with_storage().await;
        let marks = kept_marks();
        let elections = Arc::new(crate::election::Elections::new(
            LOCAL,
            Arc::new(tokio::sync::Notify::new()),
        ));
        marks.set_elections(Arc::clone(&elections));
        let handler = handler(&broker, 6, OWN).with_fleet(marks);
        let mut listed = set(5, OWN);
        for member in &mut listed.members {
            member.learner = learner && member.node_id == LOCAL;
        }

        let answer = settled(|| unified(&handler, fence_with(6, listed.clone(), None, true))).await;
        assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
        assert_eq!(elections.get(&key).is_some(), !learner, "learner {learner}");
    }
}

/// A handler on `broker` whose directory lost its ballots: no copy here
/// keeps a promise until a set readmits it, and only when `readmits` (the
/// start was checked against a registration with a storage id).
fn lost_handler(
    broker: &Arc<Broker>,
    generation: u64,
    trust_dir: &std::path::Path,
    readmits: bool,
) -> (
    ReplicaHandler,
    Arc<felix_storage::run_state::copy_trust::CopyTrust>,
) {
    lost_handler_on(broker, generation, trust_dir, readmits, marks(true))
}

/// [`lost_handler`] with the fleet as `marks` says.
fn lost_handler_on(
    broker: &Arc<Broker>,
    generation: u64,
    trust_dir: &std::path::Path,
    readmits: bool,
    marks: Arc<QuorumMarks>,
) -> (
    ReplicaHandler,
    Arc<felix_storage::run_state::copy_trust::CopyTrust>,
) {
    felix_storage::storage_identity::replace(trust_dir, OWN).expect("storage id");
    let trust =
        Arc::new(felix_storage::run_state::copy_trust::CopyTrust::open(trust_dir).expect("open"));
    marks.set_kept_ballots(false);
    marks.set_readmits(readmits);
    // As a wipe or a rotation does, unless the caller set it first.
    marks.set_minted_storage(true);
    marks.set_copy_trust(Arc::clone(&trust));
    let handler = ReplicaHandler::new(Arc::clone(broker), router_with(&[LOCAL], generation))
        .with_fleet(marks)
        .with_local_storage(StorageId::from_bytes(OWN))
        .with_stickiness(Duration::ZERO)
        .with_confirmed_routes(ConfirmedAs::new(Confirmed::Assigned {
            generation,
            leader: "broker-a".to_string(),
        }) as _);
    (handler, trust)
}

/// What a copy attests to a unified fence at 7 over the set of 6.
async fn kept_from_at_7(handler: &ReplicaHandler) -> u64 {
    let answer = settled(|| unified(handler, fence_with(7, set(6, OWN), None, false))).await;
    let InternalMessage::FenceOk(ok) = answer else {
        panic!("expected the fence to be taken, got {answer:?}");
    };
    ok.attestation.expect("attested").kept_from
}

/// **A copy on a directory that once lost its ballots counts again from the
/// set that readmitted it**, and that survives a restart. The directory's
/// lost-copy marker never clears, so without the record the copy would
/// attest no kept promise forever. A set naming it by another id, or a start
/// whose restore check has not run, readmits nothing.
#[tokio::test]
async fn a_once_lost_copy_counts_from_the_set_that_readmitted_it() {
    let name = crate::unclean::copy_key(&crate::driver::watch_key(&key()));
    for (named, readmits, readmitted) in
        [(OWN, true, Some(6)), (OLD, true, None), (OWN, false, None)]
    {
        let (broker, _dir) = broker_with_storage().await;
        let trust_dir = tempfile::tempdir().expect("tempdir");
        let (copy, trust) = lost_handler(&broker, 6, trust_dir.path(), readmits);
        wiped_at_5(&copy).await;
        let answer = settled(|| copy.fence(SENDER, readmission(6, named))).await;
        assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
        let answer =
            settled(|| copy.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream))
                .await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "{answer:?}"
        );
        assert_eq!(trust.readmitted(&name), readmitted, "named {named:?}");
        if readmitted.is_some() {
            assert!(!trust.trusted(&name), "a vouch from before the readmission");
            assert_eq!(kept_from_at_7(&copy).await, 6);
            let reopened = felix_storage::run_state::copy_trust::CopyTrust::open(trust_dir.path())
                .expect("open");
            assert_eq!(reopened.readmitted(&name), Some(6), "after a restart");
            // A start from the view reads the record too, but has not checked
            // the directory for a restore, which would have brought it back.
            let (unchecked, _) = lost_handler(&broker, 7, trust_dir.path(), false);
            assert_eq!(
                kept_from_at_7(&unchecked).await,
                u64::MAX,
                "a start from the view"
            );
        }
    }
}

/// **A directory that lost its copies without a new storage id is not
/// readmitted by a set naming that id**, as a directory swapped back after
/// the node ran under another id: sets from before the loss name the id too,
/// so nothing says this one came after it.
#[tokio::test]
async fn a_lost_copy_under_an_old_id_is_not_readmitted() {
    let name = crate::unclean::copy_key(&crate::driver::watch_key(&key()));
    for minted in [true, false] {
        let (broker, _dir) = broker_with_storage().await;
        let trust_dir = tempfile::tempdir().expect("tempdir");
        let marks = marks(true);
        marks.set_minted_storage(minted);
        let (copy, trust) = lost_handler_on(&broker, 6, trust_dir.path(), true, marks);
        let answer = settled(|| copy.fence(SENDER, fence_with(6, set(6, OWN), None, false))).await;
        assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
        let answer =
            settled(|| copy.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream))
                .await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "{answer:?}"
        );
        assert_eq!(
            trust.readmitted(&name),
            minted.then_some(6),
            "minted {minted}"
        );
    }
}

/// **A leader of the set from before the loss readmits nothing**, even at
/// a newer ballot, as a deposed leader re-shipping a wiped copy would: that
/// set names the copy by the id it lost, so its leader may lack what the old
/// copy acknowledged since (a TLC counterexample in the design).
#[tokio::test]
async fn a_deposed_leaders_batch_does_not_readmit_a_once_lost_copy() {
    let name = crate::unclean::copy_key(&crate::driver::watch_key(&key()));
    let (broker, _dir) = broker_with_storage().await;
    let trust_dir = tempfile::tempdir().expect("tempdir");
    let (copy, trust) = lost_handler(&broker, 6, trust_dir.path(), true);
    wiped_at_5(&copy).await;
    let deposed = ReplicateRecords {
        set_generation: Some(5),
        ..stamped(6, 0, &["a"])
    };
    let answer =
        settled(|| copy.apply(SENDER, deposed.clone(), felix_broker::LogKind::Stream)).await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(trust.readmitted(&name), None);
}

/// The gate of a broker that knows the fleet finalized `replica_election`.
fn electing_marks() -> Arc<QuorumMarks> {
    use felix_common::fleet::REPLICA_ELECTION;
    let fleet = FleetGate::new([JOINT_FENCE.name(), REPLICA_ELECTION.name()]);
    fleet.restart([JOINT_FENCE.name(), REPLICA_ELECTION.name()]);
    Arc::new(QuorumMarks::with_fleet(Arc::new(fleet)))
}

/// Publish `key()`'s route at 6, led by broker-a, with the control plane's
/// `readmitted` record `entries`.
fn publish_readmitted(router: &ShardRouter, entries: Vec<felix_router::Readmitted>) {
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
            member_storage_ids: Default::default(),
            pending: None,
            readmitted: entries,
        }],
        &nodes,
    );
    router.publish(table, &nodes);
}

/// What a copy attests to a unified fence at 7 over the set of 6, in full.
async fn attested_at_7(handler: &ReplicaHandler) -> felix_wire::internal::FenceAttestation {
    let answer = settled(|| unified(handler, fence_with(7, set(6, OWN), None, false))).await;
    let InternalMessage::FenceOk(ok) = answer else {
        panic!("expected the fence to be taken, got {answer:?}");
    };
    ok.attestation.expect("attested")
}

/// A once-lost copy on `trust_dir` readmitted by the set of 6 and holding
/// one record at 6, which the readmission left untrusted.
async fn readmitted_at_6(
    broker: &Arc<Broker>,
    trust_dir: &std::path::Path,
    marks: Arc<QuorumMarks>,
) -> (
    ReplicaHandler,
    Arc<felix_storage::run_state::copy_trust::CopyTrust>,
) {
    let (copy, trust) = lost_handler_on(broker, 6, trust_dir, true, marks);
    wiped_at_5(&copy).await;
    let answer = settled(|| copy.fence(SENDER, readmission(6, OWN))).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    let answer =
        settled(|| copy.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream)).await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let name = crate::unclean::copy_key(&crate::driver::watch_key(&key()));
    assert_eq!(trust.readmitted(&name), Some(6));
    assert!(!trust.trusted(&name));
    (copy, trust)
}

fn entry_for(storage: [u8; 16]) -> felix_router::Readmitted {
    felix_router::Readmitted {
        node: LOCAL.to_string(),
        storage_id: StorageId::from_bytes(storage).to_hex(),
        generation: 6,
        offset: 1,
    }
}

/// **A record vouches only for the copy it names, caught up as far as it
/// says.** The copy holds one record at 6. An entry from a later generation
/// (the copy could be missing what that generation acknowledged), past the
/// copy's end, or for another node vouches for nothing.
#[tokio::test]
async fn a_record_vouches_only_for_a_copy_that_holds_what_it_says() {
    let cases = [
        (entry_for(OWN), true),
        (
            felix_router::Readmitted {
                generation: 7,
                ..entry_for(OWN)
            },
            false,
        ),
        (
            felix_router::Readmitted {
                offset: 2,
                ..entry_for(OWN)
            },
            false,
        ),
        (
            felix_router::Readmitted {
                node: "broker-a".to_string(),
                ..entry_for(OWN)
            },
            false,
        ),
    ];
    for (entry, trusted) in cases {
        let (broker, _dir) = broker_with_storage().await;
        let trust_dir = tempfile::tempdir().expect("tempdir");
        let (copy, _trust) = readmitted_at_6(&broker, trust_dir.path(), marks(true)).await;
        publish_readmitted(&copy.router, vec![entry.clone()]);

        assert_eq!(attested_at_7(&copy).await.trusted, trusted, "{entry:?}");
    }
}

/// **The control plane's `readmitted` record vouches for a copy only in
/// place of the copy-trust vouch a clean restart loses.** A readmitted copy
/// the record names, by its current storage id and caught up to what it
/// holds, attests trusted. The same copy on a poisoned process, after an
/// unclean start no leader has vouched for, or named by the id it lost,
/// stays untrusted: the record says nothing about those.
#[tokio::test]
async fn a_record_vouches_only_for_a_copy_nothing_else_distrusts() {
    for case in ["none", "poisoned", "unclean", "other id"] {
        let (broker, _dir) = broker_with_storage().await;
        let trust_dir = tempfile::tempdir().expect("tempdir");
        let (mut copy, _trust) = readmitted_at_6(&broker, trust_dir.path(), marks(true)).await;
        let named = if case == "other id" { OLD } else { OWN };
        publish_readmitted(&copy.router, vec![entry_for(named)]);
        match case {
            "poisoned" => copy.poisoned = || true,
            "unclean" => {
                copy.unclean = Some(Arc::new(crate::unclean::UncleanStart::new(LOCAL)));
            }
            _ => {}
        }

        let attested = attested_at_7(&copy).await;

        assert_eq!(attested.kept_from, 6, "{case}");
        assert_eq!(attested.trusted, case == "none", "{case}");
        assert_eq!(attested.by_record, case == "none", "{case}");
    }
}

/// **Nor for a copy given up on disk or doubted as one this broker led
/// before a crash.** A fence from a newer leader refuses or replaces those
/// marks first, so this asks the attestation directly.
#[tokio::test]
async fn a_record_does_not_vouch_for_a_given_up_or_doubted_copy() {
    let name = crate::unclean::copy_key(&crate::driver::watch_key(&key()));
    let (broker, _dir) = broker_with_storage().await;
    let trust_dir = tempfile::tempdir().expect("tempdir");
    let (copy, trust) = readmitted_at_6(&broker, trust_dir.path(), marks(true)).await;
    assert!(copy.attestation(&key(), true, false, true).trusted);
    assert!(
        !copy.attestation(&key(), true, true, true).trusted,
        "given up"
    );
    trust.doubt(&name).expect("doubt");
    assert!(
        !copy.attestation(&key(), true, false, true).trusted,
        "doubted"
    );
}

/// **Once the fleet finalized `replica_election`, a readmission counts for
/// nothing**: neither the readmitting set's promises nor the control plane's
/// record. An adopted elected leader may have opened that set's generation
/// before the loss, which neither can tell. The copy counts as one never
/// readmitted.
#[tokio::test]
async fn a_readmission_counts_only_while_elections_are_off() {
    for electing in [false, true] {
        let (broker, _dir) = broker_with_storage().await;
        let trust_dir = tempfile::tempdir().expect("tempdir");
        let marks = if electing {
            electing_marks()
        } else {
            marks(true)
        };
        let (copy, _trust) = readmitted_at_6(&broker, trust_dir.path(), marks).await;
        publish_readmitted(&copy.router, vec![entry_for(OWN)]);

        let attested = attested_at_7(&copy).await;

        if electing {
            assert_eq!(attested, no_promise().expect("some"), "electing");
        } else {
            assert_eq!(attested.kept_from, 6);
            assert!(attested.trusted);
        }
    }
}

/// **A readmitted copy refuses a batch stamped with a set older than its
/// own**, as a deposed leader of the set from before the loss would send, and
/// its readmission stays the set that readmitted it.
#[tokio::test]
async fn a_readmitted_copy_refuses_a_batch_from_an_older_set() {
    let name = crate::unclean::copy_key(&crate::driver::watch_key(&key()));
    let (broker, _dir) = broker_with_storage().await;
    let trust_dir = tempfile::tempdir().expect("tempdir");
    let (copy, trust) = readmitted_at_6(&broker, trust_dir.path(), marks(true)).await;
    let deposed = ReplicateRecords {
        set_generation: Some(5),
        ..stamped(6, 1, &["b"])
    };

    let answer =
        settled(|| copy.apply(SENDER, deposed.clone(), felix_broker::LogKind::Stream)).await;

    assert_eq!(refusal(&answer).code, ErrorCode::StaleSet, "{answer:?}");
    assert_eq!(trust.readmitted(&name), Some(6));
    assert_eq!(log_of(&broker).await.tail_offset().await.expect("tail"), 1);
}

/// **A set that names no storage ids readmits nothing**, though it marks
/// nothing wiped either: a control plane that sends no ids could have
/// written it before the loss, so its leader may lack what the old copy
/// acknowledged.
#[tokio::test]
async fn a_set_without_storage_ids_does_not_readmit_a_once_lost_copy() {
    let name = crate::unclean::copy_key(&crate::driver::watch_key(&key()));
    let (broker, _dir) = broker_with_storage().await;
    let trust_dir = tempfile::tempdir().expect("tempdir");
    let (copy, trust) = lost_handler(&broker, 6, trust_dir.path(), true);
    let answer = settled(|| copy.fence(SENDER, fence_with(6, named(6, None), None, false))).await;
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    let answer =
        settled(|| copy.apply(SENDER, stamped(6, 0, &["a"]), felix_broker::LogKind::Stream)).await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(trust.readmitted(&name), None);
}
