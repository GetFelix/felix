//! A follower's side of the seal that moves a shard to Raft: the seal's
//! fence promises it, the promise refuses whatever could change what the
//! seal's digest covers or reopen the shard without the control plane, and
//! the seal's record is acknowledged only by the copy that holds the
//! leader's records.

use felix_storage::StorageId;
use felix_storage::disk_log::switch::{RangeDigest, SideRange, SwitchBase};
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::{
    Fence, FenceElection, PreVote, PreVoteRefusal, ReplicaLog, ReplicaSetView, ReplicateBootstrap,
    ReplicateRebuild, SealAck, SealFence, SealStarts, SetMember,
};

use super::fence::fence;
use super::groups::{put, records};
use super::*;

const SEAL: u64 = 5;
/// Every record's append time, so each copy's digest is the same.
const WRITTEN_AT: u64 = 7;

fn sealer() -> StorageId {
    StorageId::from_bytes([1; 16])
}

fn other() -> StorageId {
    StorageId::from_bytes([9; 16])
}

/// A broker with consumer groups whose logs are all fsync on commit, as a
/// copy must be to take part in the seal.
pub(super) fn sealing_broker(dir: &std::path::Path) -> Arc<Broker> {
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    config.generations.open_when(|| true);
    Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_durable_storage(
                felix_broker::DurableStorage::open(dir.join("streams"), config.clone())
                    .expect("storage"),
            )
            .with_consumer_groups(
                Arc::new(
                    felix_broker::ConsumerGroups::open(dir.join("groups"), config.clone())
                        .expect("groups"),
                ),
                Arc::new(
                    felix_broker::DeadLetters::open(dir.join("dead-letters"), config)
                        .expect("dead letters"),
                ),
                std::time::Duration::from_secs(30),
                3,
            ),
    )
}

/// `payloads` at generation 4 from offset `first`, stamped with the
/// leader's append times.
fn timed(first: u64, payloads: &[Bytes]) -> ReplicateRecords {
    ReplicateRecords {
        first_offset: first,
        times: Some(vec![WRITTEN_AT; payloads.len()]),
        ..records(4, payloads)
    }
}

/// Fill `broker`'s three logs for the shard as generation 4's leader
/// shipped them: `a b c`, one dead letter and one cursor.
pub(super) async fn holding(broker: &Arc<Broker>) {
    let handler = ReplicaHandler::new(Arc::clone(broker), router_with(&[LOCAL], 4));
    let stream = ["a", "b", "c"].map(|value| Bytes::copy_from_slice(value.as_bytes()));
    for (log, payloads) in [
        (felix_broker::LogKind::Stream, stream.to_vec()),
        (
            felix_broker::LogKind::GroupDeadLetters,
            vec![put("workers\u{1f}1", b"")],
        ),
        (
            felix_broker::LogKind::GroupCursors,
            vec![put("workers", b"cursor")],
        ),
    ] {
        let answer = handler.apply(SENDER, timed(0, &payloads), log).await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "{answer:?}"
        );
    }
}

pub(super) fn seal_fence(generation: u64, sealer: StorageId) -> SealFence {
    SealFence {
        fence: fence(generation),
        sealer: sealer.to_bytes(),
    }
}

/// A follower that holds generation 4's records and took the seal's fence
/// at generation 5 for `sealer()`.
async fn promised(dir: &std::path::Path) -> (Arc<Broker>, ReplicaHandler) {
    let broker = sealing_broker(dir);
    holding(&broker).await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], SEAL))
        .with_local_storage(StorageId::from_bytes([2; 16]))
        .with_stickiness(std::time::Duration::ZERO);
    let answer = handler.seal_fence(SENDER, seal_fence(SEAL, sealer())).await;
    let InternalMessage::SealFenceOk(ok) = answer else {
        panic!("expected the seal's fence to be taken, got {answer:?}");
    };
    assert_eq!(ok.starts, SealStarts::default());
    assert_eq!(ok.ok.log_end, 3);
    (broker, handler)
}

fn refused_for(answer: &InternalMessage, why: &str) {
    let err = refusal(answer);
    assert_eq!(err.code, ErrorCode::FencedEpoch, "{err:?}");
    assert!(err.detail.contains(why), "{err:?}");
}

/// **The seal's fence is a promise on disk**, answered with where the
/// copy's three logs start. It survives a restart, like the ballot it rides
/// on.
#[tokio::test]
async fn the_seals_fence_promises_the_seal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, handler) = promised(dir.path()).await;
    let promise = handler
        .seal_promise(&batch(SEAL, 0, &[]).shard)
        .await
        .expect("a promise");
    assert_eq!((promise.generation, promise.sealer), (SEAL, sealer()));
    drop((handler, broker));

    let broker = sealing_broker(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], SEAL));
    assert!(
        handler
            .seal_promise(&batch(SEAL, 0, &[]).shard)
            .await
            .is_some()
    );
}

/// **A promised copy takes batches only from the sealing copy.** The same
/// leader under another copy id, as after a wipe, or a sender that names no
/// copy, could write past what the seal's digest covers.
#[tokio::test]
async fn a_promised_copy_takes_batches_only_from_the_sealer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, handler) = promised(dir.path()).await;
    // From the commit offset, as a new generation's leader resends.
    let next = || ReplicateRecords {
        times: Some(vec![WRITTEN_AT; 4]),
        ..batch(SEAL, 0, &["a", "b", "c", "d"])
    };
    let stream = felix_broker::LogKind::Stream;

    let bare = handler.apply(SENDER, next(), stream).await;
    assert!(
        refusal(&bare).detail.contains("only from the sealing copy"),
        "{bare:?}"
    );
    refused_for(
        &handler
            .apply_from(SENDER, Some(other()), next(), stream)
            .await,
        "only from the sealing copy",
    );
    assert_eq!(held(&broker).await, vec!["a", "b", "c"]);

    let answer = handler
        .apply_from(SENDER, Some(sealer()), next(), stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(held(&broker).await, vec!["a", "b", "c", "d"]);
}

/// **A batch the leader sent before its own promise is retried, not
/// fenced.** The leader promises its own copy and then fences the
/// followers, so a batch already on its way names no copy and can land
/// after this copy's promise. A fenced answer halts the follower for the
/// whole generation, and with both followers of three halted the seal
/// never gets its majority. The leader's next batch names its copy.
#[tokio::test]
async fn a_batch_sent_before_the_leaders_own_promise_is_retried() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, handler) = promised(dir.path()).await;
    let next = || ReplicateRecords {
        times: Some(vec![WRITTEN_AT; 4]),
        ..batch(SEAL, 0, &["a", "b", "c", "d"])
    };
    let stream = felix_broker::LogKind::Stream;

    let bare = handler.apply(SENDER, next(), stream).await;
    assert_eq!(refusal(&bare).code, ErrorCode::Unavailable, "{bare:?}");
    assert_eq!(crate::read_answer(&bare), crate::Progress::Retry);
    assert_eq!(held(&broker).await, vec!["a", "b", "c"]);

    // Another copy's batch is a fence: that leader was wiped.
    let wiped = handler
        .apply_from(SENDER, Some(other()), next(), stream)
        .await;
    assert_eq!(
        crate::read_answer(&wiped),
        crate::Progress::Halted(crate::Halt::Fenced)
    );

    let stamped = handler
        .apply_from(SENDER, Some(sealer()), next(), stream)
        .await;
    assert!(
        matches!(crate::read_answer(&stamped), crate::Progress::Stored { .. }),
        "{stamped:?}"
    );
    assert_eq!(held(&broker).await, vec!["a", "b", "c", "d"]);
}

/// **A promised copy takes no rebuild and no bootstrap**: either discards
/// records the seal's digest covers.
#[tokio::test]
async fn a_promised_copy_takes_no_rebuild_or_bootstrap() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, handler) = promised(dir.path()).await;
    let shard = batch(SEAL, 0, &[]).shard;

    let rebuild = handler
        .rebuild(
            SENDER,
            ReplicateRebuild {
                correlation_id: 1,
                shard: shard.clone(),
                log: ReplicaLog::Stream,
                base_offset: 0,
            },
        )
        .await;
    refused_for(&rebuild, "takes no rebuild");
    let bootstrap = handler
        .bootstrap(
            SENDER,
            ReplicateBootstrap {
                correlation_id: 1,
                shard,
                base_offset: 0,
            },
            felix_broker::LogKind::Stream,
        )
        .await;
    refused_for(&bootstrap, "takes no rebuild");
    assert_eq!(held(&broker).await, vec!["a", "b", "c"]);
}

/// **A later generation's rebuild is not refused by the promise.** A copy
/// that missed the later generation's fence still holds the promise, and a
/// side log's ballot never ends it. Refused as sealed, the copy would stay
/// out of the reopened shard until something else fenced its stream log.
#[tokio::test]
async fn a_later_generations_rebuild_passes_the_promise() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _sealed) = promised(dir.path()).await;
    let later = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], SEAL + 1))
        .with_local_storage(StorageId::from_bytes([2; 16]))
        .with_stickiness(std::time::Duration::ZERO);
    let shard = batch(SEAL + 1, 0, &[]).shard;
    let rebuild = |log| ReplicateRebuild {
        correlation_id: 1,
        shard: shard.clone(),
        log,
        base_offset: 0,
    };

    for log in [ReplicaLog::GroupCursors, ReplicaLog::GroupDeadLetters] {
        let answer = later.rebuild(SENDER, rebuild(log)).await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "{log:?}: {answer:?}"
        );
    }
    let bootstrap = later
        .bootstrap(
            SENDER,
            ReplicateBootstrap {
                correlation_id: 1,
                shard: shard.clone(),
                base_offset: 0,
            },
            felix_broker::LogKind::GroupCursors,
        )
        .await;
    assert!(
        matches!(bootstrap, InternalMessage::ReplicateOk(_)),
        "{bootstrap:?}"
    );
    // Only the stream log's own ballot ends the promise.
    assert!(later.seal_promise(&shard).await.is_some());

    let stream = later.rebuild(SENDER, rebuild(ReplicaLog::Stream)).await;
    if let InternalMessage::ReplicateError(err) = &stream {
        assert!(!err.detail.contains("sealed"), "{err:?}");
    }
    assert!(later.seal_promise(&shard).await.is_none());
}

fn election_fence(generation: u64, candidate: bool) -> Fence {
    let member = |node: &str| SetMember {
        node_id: node.to_string(),
        storage_id: None,
        learner: false,
    };
    Fence {
        election: Some(FenceElection {
            candidate,
            set: ReplicaSetView {
                generation: 1,
                members: ["broker-a", LOCAL, "broker-c"].map(member).to_vec(),
            },
            leaving: None,
            earlier: Vec::new(),
        }),
        ..fence(generation)
    }
}

fn pre_vote(generation: u64) -> PreVote {
    PreVote {
        correlation_id: 3,
        shard: batch(generation, 0, &[]).shard,
        log: ReplicaLog::Stream,
        last_generation: SEAL,
        log_end: 1 << 20,
        set_generation: 0,
    }
}

/// **A promised copy elects nothing.** An election among sealed copies
/// would reopen the shard without the control plane, so the copy refuses
/// an election's ballot and does not grant a pre-vote. A later generation
/// promoted by the control plane ends the promise, and with it both
/// refusals. A promised copy never stands as a candidate either
/// (`election::candidate::eligible_log`).
#[tokio::test]
async fn a_promised_copy_refuses_elections_until_a_later_generation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_broker, handler) = promised(dir.path()).await;

    refused_for(
        &handler.fence(SENDER, election_fence(SEAL + 1, true)).await,
        "takes no election ballot",
    );
    let InternalMessage::PreVoteAnswer(answer) = handler.pre_vote(pre_vote(SEAL + 1)).await else {
        panic!("expected a pre-vote answer");
    };
    assert_eq!(answer.refusal, Some(PreVoteRefusal::NotVoting));

    // A control-plane promotion names the set too, with no candidate: it
    // is how the shard reopens, so the promise does not refuse it.
    let promoted = handler.fence(SENDER, election_fence(SEAL + 1, false)).await;
    super::fence::taken(&promoted);
    assert!(
        handler
            .seal_promise(&batch(SEAL, 0, &[]).shard)
            .await
            .is_none()
    );
    let InternalMessage::PreVoteAnswer(answer) = handler.pre_vote(pre_vote(SEAL + 2)).await else {
        panic!("expected a pre-vote answer");
    };
    assert_ne!(answer.refusal, Some(PreVoteRefusal::NotVoting));
}

/// **A seal's record is acknowledged only for the copy the promise
/// names**, and only if it is a seal's record at all.
#[tokio::test]
async fn a_seal_ack_from_another_copy_or_of_another_record_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, handler) = promised(dir.path()).await;
    let ack = |sealer: StorageId, payload: Bytes| SealAck {
        correlation_id: 4,
        shard: batch(SEAL, 0, &[]).shard,
        sealer: sealer.to_bytes(),
        offset: 3,
        timestamp_micros: WRITTEN_AT,
        payload,
    };
    let side = SideRange {
        start: 0,
        end: 1,
        digest: RangeDigest([0; 32]),
    };
    // A seal's record whose digests this copy does not match.
    let seal = SwitchBase {
        generation: SEAL,
        offset: 3,
        start: 0,
        digest: RangeDigest([0; 32]),
        dead_letters: side,
        cursors: side,
    }
    .record(WRITTEN_AT)
    .payload;

    let from_other = handler.seal_ack(SENDER, ack(other(), seal.clone())).await;
    refused_for(&from_other, "PromisedOther");
    let differs = handler.seal_ack(SENDER, ack(sealer(), seal)).await;
    assert_eq!(refusal(&differs).code, ErrorCode::LogConflict);
    assert!(refusal(&differs).detail.contains("Differs"), "{differs:?}");
    let not_a_seal = handler
        .seal_ack(SENDER, ack(sealer(), Bytes::from_static(b"nope")))
        .await;
    assert_eq!(refusal(&not_a_seal).code, ErrorCode::LogConflict);
    assert!(
        refusal(&not_a_seal).detail.contains("NotASeal"),
        "{not_a_seal:?}"
    );
    assert_eq!(held(&broker).await, vec!["a", "b", "c"]);
}

/// **A build or a copy without the side logs promises nothing**, and the
/// seal's fence never carries an election.
#[tokio::test]
async fn a_seal_fence_without_side_logs_or_with_an_election_is_refused() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    handler
        .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], SEAL));
    let answer = handler.seal_fence(SENDER, seal_fence(SEAL, sealer())).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    assert!(
        handler
            .seal_promise(&batch(SEAL, 0, &[]).shard)
            .await
            .is_none()
    );

    let elected = SealFence {
        fence: election_fence(SEAL, true),
        sealer: sealer().to_bytes(),
    };
    let answer = handler.seal_fence(SENDER, elected).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Malformed);
}

/// Followers reached in process: each seal message goes to the named
/// node's handler, as the pool would send it, and a node that did not offer
/// the seal is refused before anything is sent.
struct Followers<'a> {
    nodes: Vec<(&'static str, &'a ReplicaHandler, PeerCapabilities)>,
    /// While false, every `SealAck` is lost on the way.
    acking: std::sync::atomic::AtomicBool,
    /// The leader's log and a generation it accepts while its first
    /// `SealAck` is on the way: after the round's claim, before its report.
    later: Option<(&'a felix_broker::StreamLog, u64)>,
}

impl crate::peer::PeerRequester for Followers<'_> {
    async fn request(
        &self,
        node_id: &str,
        _addr: std::net::SocketAddr,
        message: InternalMessage,
    ) -> Result<InternalMessage, crate::peer::PeerError> {
        let (_, handler, offered) = self
            .nodes
            .iter()
            .find(|(node, _, _)| *node == node_id)
            .expect("a follower");
        if let Some(needed) = crate::peer::pool::required_capability(&message)
            && !offered.contains(needed)
        {
            return Err(crate::peer::PeerError::Unsupported {
                node_id: node_id.to_string(),
                kind: message.kind(),
            });
        }
        if matches!(message, InternalMessage::SealAck(_))
            && !self.acking.load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(crate::peer::PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "lost".to_string(),
            });
        }
        if let (InternalMessage::SealAck(_), Some((leader, generation))) = (&message, self.later) {
            leader
                .accept_generation(generation, Some("broker-a"))
                .await
                .expect("ballot");
        }
        Ok(match message {
            InternalMessage::SealFence(request) => handler.seal_fence(SENDER, request).await,
            InternalMessage::SealAck(request) => handler.seal_ack(SENDER, request).await,
            other => panic!("the seal leader sent {:?}", other.kind()),
        })
    }
}

/// A follower copy: its broker and its handler at the seal's generation.
async fn follower(dir: &std::path::Path, copy: u8) -> (Arc<Broker>, ReplicaHandler) {
    let broker = sealing_broker(dir);
    holding(&broker).await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], SEAL))
        .with_local_storage(StorageId::from_bytes([copy; 16]));
    (broker, handler)
}

async fn logs_of(broker: &Broker) -> [felix_broker::StreamLog; 3] {
    let log = |kind| broker.shard_log(kind, TENANT, NAMESPACE, STREAM, 0);
    [
        log(felix_broker::LogKind::Stream).await.expect("log"),
        log(felix_broker::LogKind::GroupDeadLetters)
            .await
            .expect("dead letters"),
        log(felix_broker::LogKind::GroupCursors)
            .await
            .expect("cursors"),
    ]
}

/// **The seal leader's round, end to end against real followers.** It
/// claims nothing while a write may still be in flight; once quiesced it
/// claims the record at `N`, each follower that offered the seal checks its
/// copy and stores the record, and the report names the majority by copy
/// id. A follower that did not offer the seal is never sent it.
#[tokio::test]
async fn the_seal_leader_claims_once_quiesced_and_reports_the_copies_that_hold_the_record() {
    let root = tempfile::tempdir().expect("tempdir");
    let leader = sealing_broker(&root.path().join("a"));
    holding(&leader).await;
    let (b_broker, b) = follower(&root.path().join("b"), 2).await;
    let (c_broker, c) = follower(&root.path().join("c"), 3).await;
    let (d_broker, d) = follower(&root.path().join("d"), 4).await;
    let followers = Followers {
        nodes: vec![
            ("broker-b", &b, PeerCapabilities::SWITCH_SEAL),
            ("broker-c", &c, PeerCapabilities::SWITCH_SEAL),
            ("broker-d", &d, PeerCapabilities::NONE),
        ],
        acking: true.into(),
        later: None,
    };
    let addr: std::net::SocketAddr = "10.0.0.1:7000".parse().expect("addr");
    let peers: Vec<_> = ["broker-b", "broker-c", "broker-d"]
        .map(|node| (node.to_string(), addr))
        .to_vec();
    let [log, dead_letters, cursors] = logs_of(&leader).await;
    let logs = crate::seal::SealLogs {
        log: log.disk_log(),
        dead_letters: dead_letters.disk_log(),
        cursors: cursors.disk_log(),
    };
    let mut round = crate::seal::SealRound::new(
        SEAL,
        ["broker-b", "broker-c", "broker-d"].map(String::from),
        ("broker-a".to_string(), sealer()),
        crate::seal::LogStarts::default(),
    );
    let shard = batch(SEAL, 0, &[]).shard;

    let waiting = round
        .step(&followers, &shard, &peers, logs, false)
        .await
        .expect("step");
    assert_eq!(waiting, crate::seal::SealProgress::Waiting);
    assert_eq!(log.tail_offset().await.expect("tail"), 3, "claimed early");
    for handler in [&b, &c] {
        assert!(handler.seal_promise(&shard).await.is_some());
    }
    assert!(d.seal_promise(&shard).await.is_none());

    let crate::seal::SealProgress::Reported(report) = round
        .step(&followers, &shard, &peers, logs, true)
        .await
        .expect("step")
    else {
        panic!("expected a report");
    };
    assert_eq!((report.base.offset, report.base.start), (3, 0));
    assert_eq!(
        report.voters,
        [
            ("broker-a".to_string(), sealer()),
            ("broker-b".to_string(), StorageId::from_bytes([2; 16])),
            ("broker-c".to_string(), StorageId::from_bytes([3; 16])),
        ]
        .into_iter()
        .collect()
    );
    for broker in [&b_broker, &c_broker] {
        let [log, _, _] = logs_of(broker).await;
        assert_eq!(log.tail_offset().await.expect("tail"), 4);
    }
    let [log_d, _, _] = logs_of(&d_broker).await;
    assert_eq!(log_d.tail_offset().await.expect("tail"), 3);
}

/// **A copy that does not fsync on commit promises nothing.** It could
/// never acknowledge `N`, so answering would count it toward the claim's
/// majority for nothing.
#[tokio::test]
async fn a_copy_that_does_not_fsync_on_commit_refuses_the_seals_fence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = super::groups::group_broker(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    handler
        .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], SEAL));
    let answer = handler.seal_fence(SENDER, seal_fence(SEAL, sealer())).await;
    assert!(
        refusal(&answer).detail.contains("fsync on commit"),
        "{answer:?}"
    );
    assert!(
        handler
            .seal_promise(&batch(SEAL, 0, &[]).shard)
            .await
            .is_none()
    );
}

/// **A leader whose own promise lapsed after the claim reports nothing.**
/// A later generation it accepts while the acknowledgements are on the way
/// reopened the shard, though both followers, which have not seen it, hold
/// the record at `N` and answered.
#[tokio::test]
async fn a_seal_leader_that_accepted_a_later_generation_reports_nothing() {
    let root = tempfile::tempdir().expect("tempdir");
    let leader = sealing_broker(&root.path().join("a"));
    holding(&leader).await;
    let (b_broker, b) = follower(&root.path().join("b"), 2).await;
    let (c_broker, c) = follower(&root.path().join("c"), 3).await;
    let [log, dead_letters, cursors] = logs_of(&leader).await;
    let followers = Followers {
        nodes: vec![
            ("broker-b", &b, PeerCapabilities::SWITCH_SEAL),
            ("broker-c", &c, PeerCapabilities::SWITCH_SEAL),
        ],
        acking: true.into(),
        later: Some((&log, SEAL + 1)),
    };
    let addr: std::net::SocketAddr = "10.0.0.1:7000".parse().expect("addr");
    let peers: Vec<_> = ["broker-b", "broker-c"]
        .map(|node| (node.to_string(), addr))
        .to_vec();
    let logs = crate::seal::SealLogs {
        log: log.disk_log(),
        dead_letters: dead_letters.disk_log(),
        cursors: cursors.disk_log(),
    };
    let mut round = crate::seal::SealRound::new(
        SEAL,
        ["broker-b", "broker-c"].map(String::from),
        ("broker-a".to_string(), sealer()),
        crate::seal::LogStarts::default(),
    );
    let shard = batch(SEAL, 0, &[]).shard;

    let after = round
        .step(&followers, &shard, &peers, logs, true)
        .await
        .expect("step");
    assert_eq!(
        after,
        crate::seal::SealProgress::Refused(
            felix_storage::disk_log::switch::seal::SealRefused::NoPromise
        )
    );
    // Everything a report needs was there: the claim and a majority's
    // acknowledgements.
    assert_eq!(log.accepted_generation(), SEAL + 1);
    for broker in [&b_broker, &c_broker] {
        let [log, _, _] = logs_of(broker).await;
        assert_eq!(log.tail_offset().await.expect("tail"), 4);
    }
    assert_eq!(
        round
            .step(&followers, &shard, &peers, logs, true)
            .await
            .expect("step"),
        crate::seal::SealProgress::Abandoned
    );
}

/// The record a leader holding the same records claimed for the seal, as
/// the acknowledgement request it sends.
async fn claimed(dir: &std::path::Path) -> SealAck {
    use felix_storage::log::{AppendOnlyLog, ReadRange};
    let leader = sealing_broker(dir);
    holding(&leader).await;
    let [log, dead_letters, cursors] = logs_of(&leader).await;
    log.disk_log()
        .promise_seal(SEAL, "broker-a", sealer())
        .await
        .expect("promise")
        .expect("one sealer");
    let base = log
        .disk_log()
        .claim_seal(
            sealer(),
            0,
            (dead_letters.disk_log(), 0),
            (cursors.disk_log(), 0),
        )
        .await
        .expect("claim")
        .expect("claimed");
    let record = log
        .disk_log()
        .read_range_with_trims(ReadRange {
            start: base.offset,
            max_bytes: 1,
        })
        .await
        .expect("read")
        .remove(0);
    SealAck {
        correlation_id: 4,
        shard: batch(SEAL, 0, &[]).shard,
        sealer: sealer().to_bytes(),
        offset: record.offset,
        timestamp_micros: record.timestamp_micros,
        payload: record.payload,
    }
}

/// **A check that outlasts its request still finishes, once.** Reading
/// every record from the seal's start can take longer than a request may
/// wait. Each request is answered in time as retryable, the retries join
/// the one check instead of starting their own, and the request after it
/// finishes gets the acknowledgement.
#[tokio::test]
async fn a_slow_seal_check_answers_as_running_and_finishes_once() {
    use std::sync::atomic::Ordering;
    let root = tempfile::tempdir().expect("tempdir");
    let ack = claimed(&root.path().join("a")).await;
    let (broker, mut handler) = promised(&root.path().join("b")).await;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    handler.seal_acks.gate = Some(Arc::clone(&gate));
    handler.seal_acks.answer_wait = std::time::Duration::from_millis(20);

    for _ in 0..3 {
        let answer = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            handler.seal_ack(SENDER, ack.clone()),
        )
        .await
        .expect("a request waited out the whole check");
        assert_eq!(refusal(&answer).code, ErrorCode::Unavailable, "{answer:?}");
        assert_eq!(refusal(&answer).correlation_id, 4);
    }
    assert_eq!(
        handler.seal_acks.started.load(Ordering::SeqCst),
        1,
        "each retry started its own check"
    );
    // Another seal's record waits for the running check.
    let another = SealAck {
        offset: ack.offset + 1,
        ..ack.clone()
    };
    let answer = handler.seal_ack(SENDER, another).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Unavailable, "{answer:?}");
    assert_eq!(handler.seal_acks.started.load(Ordering::SeqCst), 1);

    gate.add_permits(8);
    let mut answer = handler.seal_ack(SENDER, ack.clone()).await;
    for _ in 0..500 {
        if !matches!(answer, InternalMessage::ReplicateError(_)) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        answer = handler.seal_ack(SENDER, ack.clone()).await;
    }
    let InternalMessage::SealAckOk(ok) = answer else {
        panic!("expected the acknowledgement, got {answer:?}");
    };
    assert_eq!(ok.copy, [2; 16]);
    assert_eq!(handler.seal_acks.started.load(Ordering::SeqCst), 1);
    let [log, _, _] = logs_of(&broker).await;
    assert_eq!(log.tail_offset().await.expect("tail"), ack.offset + 1);

    // Its result was read, so a repeat checks the copy again.
    let again = handler.seal_ack(SENDER, ack).await;
    assert!(matches!(again, InternalMessage::SealAckOk(_)), "{again:?}");
    assert_eq!(handler.seal_acks.started.load(Ordering::SeqCst), 2);
}

/// **A result nobody waited for is not an acknowledgement after a later
/// generation.** The check finished under the promise; the request that
/// reads it comes after the shard reopened.
#[tokio::test]
async fn a_finished_check_is_not_acknowledged_once_the_promise_ended() {
    let root = tempfile::tempdir().expect("tempdir");
    let ack = claimed(&root.path().join("a")).await;
    let (broker, mut handler) = promised(&root.path().join("b")).await;
    handler.seal_acks.answer_wait = std::time::Duration::ZERO;
    let [log, _, _] = logs_of(&broker).await;

    let running = handler.seal_ack(SENDER, ack.clone()).await;
    assert_eq!(refusal(&running).code, ErrorCode::Unavailable);
    for _ in 0..500 {
        if log.tail_offset().await.expect("tail") == ack.offset + 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(log.tail_offset().await.expect("tail"), ack.offset + 1);
    log.accept_generation(SEAL + 1, Some("broker-a"))
        .await
        .expect("ballot");

    handler.seal_acks.answer_wait = std::time::Duration::from_secs(10);
    let answer = handler.seal_ack(SENDER, ack).await;
    refused_for(&answer, "NoPromise");
}

struct NoServing;

impl crate::quorum::ShardServing for NoServing {
    fn replicated(&self, _: &crate::ShardKey) -> bool {
        false
    }

    fn generation(&self, _: &crate::ShardKey) -> Option<u64> {
        None
    }

    fn lease_valid(&self) -> bool {
        true
    }

    fn record_ack_refusal(&self) {}
}

/// **The engine hands a `FromCopy` batch to the replica with its copy
/// id**, and refuses anything else inside the envelope. Without the id the
/// sealer's catch-up would be refused like a bare batch.
#[tokio::test]
async fn the_engine_dispatches_a_batch_named_by_a_copy() {
    use crate::engine::Engine;
    use felix_wire::internal::FromCopy;
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, handler) = promised(dir.path()).await;
    let engine = crate::engine::Hybrid::<NoServing>::default();
    assert!(engine.set_replica(handler).is_ok());
    let from = |copy: StorageId, message| {
        InternalMessage::FromCopy(FromCopy {
            copy: copy.to_bytes(),
            message: Box::new(message),
        })
    };
    let next = || {
        InternalMessage::ReplicateRecords(ReplicateRecords {
            times: Some(vec![WRITTEN_AT; 4]),
            ..batch(SEAL, 0, &["a", "b", "c", "d"])
        })
    };
    let seal = PeerCapabilities::SWITCH_SEAL;

    let wiped = engine.handle(SENDER, seal, from(other(), next())).await;
    refused_for(&wiped, "only from the sealing copy");
    let stored = engine.handle(SENDER, seal, from(sealer(), next())).await;
    assert!(
        matches!(stored, InternalMessage::ReplicateOk(_)),
        "{stored:?}"
    );
    assert_eq!(held(&broker).await, vec!["a", "b", "c", "d"]);

    let bootstrap = InternalMessage::ReplicateBootstrap(ReplicateBootstrap {
        correlation_id: 1,
        shard: batch(SEAL, 0, &[]).shard,
        base_offset: 0,
    });
    let InternalMessage::ForwardPublishError(err) =
        engine.handle(SENDER, seal, from(sealer(), bootstrap)).await
    else {
        panic!("expected the envelope's bootstrap to be refused");
    };
    assert_eq!(err.code, ErrorCode::Malformed);
}
