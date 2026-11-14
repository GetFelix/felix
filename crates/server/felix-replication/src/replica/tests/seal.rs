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
    holding_records(broker, ["a", "b", "c"]).await;
}

/// As [`holding`], with `stream` as the stream log's three records.
pub(super) async fn holding_records(broker: &Arc<Broker>, stream: [&str; 3]) {
    let handler = ReplicaHandler::new(Arc::clone(broker), router_with(&[LOCAL], 4));
    let stream = stream.map(|value| Bytes::copy_from_slice(value.as_bytes()));
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

    refused_for(
        &handler.apply(SENDER, next(), stream).await,
        "only from the sealing copy",
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

fn election_fence(generation: u64) -> Fence {
    let member = |node: &str| SetMember {
        node_id: node.to_string(),
        storage_id: None,
        learner: false,
    };
    Fence {
        election: Some(FenceElection {
            candidate: true,
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
/// refusals.
#[tokio::test]
async fn a_promised_copy_refuses_elections_until_a_later_generation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_broker, handler) = promised(dir.path()).await;

    refused_for(
        &handler.fence(SENDER, election_fence(SEAL + 1)).await,
        "takes no election ballot",
    );
    let InternalMessage::PreVoteAnswer(answer) = handler.pre_vote(pre_vote(SEAL + 1)).await else {
        panic!("expected a pre-vote answer");
    };
    assert_eq!(answer.refusal, Some(PreVoteRefusal::NotVoting));

    super::fence::taken(&handler.fence(SENDER, fence(SEAL + 1)).await);
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
        fence: election_fence(SEAL),
        sealer: sealer().to_bytes(),
    };
    let answer = handler.seal_fence(SENDER, elected).await;
    assert_eq!(refusal(&answer).code, ErrorCode::Malformed);
}

/// Followers reached in process: each seal message goes to the named
/// node's handler, as the pool would send it, and a node that did not offer
/// the seal is refused before anything is sent.
pub(super) struct Followers<'a> {
    pub(super) nodes: Vec<(&'static str, &'a ReplicaHandler, PeerCapabilities)>,
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
        Ok(match message {
            InternalMessage::SealFence(request) => handler.seal_fence(SENDER, request).await,
            InternalMessage::SealAck(request) => handler.seal_ack(SENDER, request).await,
            other => panic!("the seal leader sent {:?}", other.kind()),
        })
    }
}

/// A follower copy: its broker and its handler at the seal's generation.
pub(super) async fn follower(dir: &std::path::Path, copy: u8) -> (Arc<Broker>, ReplicaHandler) {
    let broker = sealing_broker(dir);
    holding(&broker).await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], SEAL))
        .with_local_storage(StorageId::from_bytes([copy; 16]));
    (broker, handler)
}

pub(super) async fn logs_of(broker: &Broker) -> [felix_broker::StreamLog; 3] {
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
