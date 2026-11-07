//! What a probe of a given-up shard's followers reports.
use std::collections::HashMap;
use std::net::SocketAddr;

use bytes::Bytes;
use felix_broker::DurableStorage;
use felix_router::NodeRef;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::{ErrorCode, FenceOk, RefusalReason, ReplicateError};

use super::*;
use crate::peer::{PeerError, PeerIdentity};

const LOCAL: &str = "broker-a";

/// What a follower that does not vote answers with, in place of a log end.
const NOT_A_VOTER: u64 = u64::MAX;

/// Followers answering a fence with where their copies end, refusing it as a
/// non-voter (`NOT_A_VOTER`), or not at all.
struct Followers(HashMap<&'static str, Option<u64>>);

impl PeerRequester for Followers {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        assert!(matches!(message, InternalMessage::Fence(_)), "{message:?}");
        match self.0.get(node_id).copied().flatten() {
            Some(NOT_A_VOTER) => Ok(InternalMessage::ReplicateError(ReplicateError {
                correlation_id: 0,
                code: ErrorCode::Unauthorized,
                expected_offset: 0,
                detail: "this broker does not vote until a set change re-admits it".to_string(),
                reason: Some(RefusalReason::NotVoter),
            })),
            Some(log_end) => Ok(InternalMessage::FenceOk(FenceOk {
                correlation_id: 0,
                log_end,
                commit_offset: 0,
                last_generation: 0,
                kept_ballot: false,
                attestation: None,
            })),
            None => Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "down".to_string(),
            }),
        }
    }

    async fn identity(
        &self,
        node_id: &str,
        _addr: SocketAddr,
    ) -> std::result::Result<PeerIdentity, PeerError> {
        Ok(PeerIdentity {
            storage: Some(storage_of(node_id)),
            incarnation: Incarnation::At(7),
        })
    }
}

fn storage_of(node_id: &str) -> StorageId {
    StorageId::from_bytes([node_id.len() as u8; 16])
}

fn key() -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    }
}

fn node(node_id: &str, port: u16) -> NodeRef {
    NodeRef {
        node_id: node_id.to_string(),
        advertise_addr: format!("127.0.0.1:{port}").parse().expect("addr"),
        region: "local".to_string(),
        live: true,
    }
}

fn route() -> Route {
    Route {
        leader: node(LOCAL, 7001),
        replicas: vec![
            node(LOCAL, 7001),
            node("broker-b", 7002),
            node("broker-c", 7003),
        ],
        generation: 4,
        draining: false,
        successor: None,
        incarnation: 0,
        fence_storage_ids: None,
        member_storage_ids: None,
        pending: None,
    }
}

/// This broker, holding `count` records of the shard.
async fn holding(count: usize) -> (Arc<Broker>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let log = storage.open_stream("t1", "ns", "orders", 0).expect("open");
    for i in 0..count {
        log.append(&[Bytes::from(format!("v{i}"))])
            .await
            .expect("append");
    }
    (
        Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage)),
        dir,
    )
}

/// **Every follower behind: the copy may be reclaimed**, and the report
/// names nobody caught up, with this broker's own position beside every
/// follower's.
#[tokio::test]
async fn no_follower_ahead_is_reclaimable() {
    let (broker, _dir) = holding(5).await;
    let followers = Followers(HashMap::from([
        ("broker-b", Some(4)),
        ("broker-c", Some(3)),
    ]));

    let probe = probe(&followers, &broker, LOCAL, &key(), &route())
        .await
        .expect("every follower answered");

    assert!(probe.reclaimable);
    let report = probe.report;
    assert!(report.given_up && !report.drained);
    assert_eq!(report.generation, 4);
    assert!(report.caught_up.is_empty(), "{:?}", report.caught_up);
    let mut offsets = report.offsets.clone();
    offsets.sort();
    assert_eq!(
        offsets,
        vec![
            ("broker-a".to_string(), 5),
            ("broker-b".to_string(), 4),
            ("broker-c".to_string(), 3)
        ]
    );
    assert!(
        report
            .storage_ids
            .contains(&("broker-b".to_string(), storage_of("broker-b")))
    );
    assert!(
        report
            .node_incarnations
            .contains(&("broker-c".to_string(), Some(7)))
    );
}

/// **A follower level with this broker is reported caught up**, so placement
/// promotes it rather than naming this broker again (#1314). The copy stays
/// reclaimable here: nothing is held beyond it.
#[tokio::test]
async fn a_level_follower_is_reported_caught_up() {
    let (broker, _dir) = holding(5).await;
    let followers = Followers(HashMap::from([
        ("broker-b", Some(5)),
        ("broker-c", Some(3)),
    ]));

    let probe = probe(&followers, &broker, LOCAL, &key(), &route())
        .await
        .expect("every follower answered");

    assert!(probe.reclaimable);
    assert_eq!(probe.report.caught_up, vec!["broker-b".to_string()]);
}

/// **A follower ahead holds what this broker lost**: not reclaimable, and
/// the furthest copy is reported caught up, every copy being accounted for.
#[tokio::test]
async fn a_follower_ahead_is_reported_caught_up() {
    let (broker, _dir) = holding(5).await;
    let followers = Followers(HashMap::from([
        ("broker-b", Some(8)),
        ("broker-c", Some(6)),
    ]));

    let probe = probe(&followers, &broker, LOCAL, &key(), &route())
        .await
        .expect("every follower answered");

    assert!(!probe.reclaimable);
    assert_eq!(probe.report.caught_up, vec!["broker-b".to_string()]);
}

/// **A follower not heard from may hold what this broker lost**, so a probe
/// it did not answer reports nothing at all.
#[tokio::test]
async fn a_follower_not_heard_from_reports_nothing() {
    let (broker, _dir) = holding(5).await;
    let followers = Followers(HashMap::from([("broker-b", Some(2)), ("broker-c", None)]));

    assert!(
        probe(&followers, &broker, LOCAL, &key(), &route())
            .await
            .is_none()
    );
}

/// **A follower that refuses as a non-voter has no position** (#2066). With
/// three replicas the one follower that counts does not meet every majority,
/// so the report is held: nobody is named caught up, the copy is not
/// reclaimable, and the refuser's offset is left out.
#[tokio::test]
async fn a_non_voting_follower_holds_the_shard() {
    let (broker, _dir) = holding(5).await;
    let followers = Followers(HashMap::from([
        ("broker-b", Some(8)),
        ("broker-c", Some(NOT_A_VOTER)),
    ]));

    let probe = probe(&followers, &broker, LOCAL, &key(), &route())
        .await
        .expect("both followers answered");

    assert!(!probe.reclaimable);
    assert!(probe.report.given_up);
    assert!(
        probe.report.caught_up.is_empty(),
        "{:?}",
        probe.report.caught_up
    );
    let mut offsets = probe.report.offsets.clone();
    offsets.sort();
    assert_eq!(
        offsets,
        vec![("broker-a".to_string(), 5), ("broker-b".to_string(), 8)]
    );
    assert_eq!(probe.report.storage_ids.len(), 1);
}

/// A follower behind this broker, beside a refuser, does not make the copy
/// reclaimable either: the refuser may hold what neither has.
#[tokio::test]
async fn a_non_voting_follower_blocks_a_reclaim() {
    let (broker, _dir) = holding(5).await;
    let followers = Followers(HashMap::from([
        ("broker-b", Some(3)),
        ("broker-c", Some(NOT_A_VOTER)),
    ]));

    let probe = probe(&followers, &broker, LOCAL, &key(), &route())
        .await
        .expect("both followers answered");

    assert!(!probe.reclaimable);
    assert!(probe.report.caught_up.is_empty());
}

/// A silent follower beside a refuser: the silent one, answering, could still
/// make the counted followers meet every majority, so nothing is reported.
/// Once the answers in hand make that impossible, the held report goes.
#[tokio::test]
async fn silence_waits_unless_the_verdict_is_already_held() {
    let (broker, _dir) = holding(5).await;
    let waiting = Followers(HashMap::from([("broker-b", Some(8)), ("broker-c", None)]));
    assert!(
        probe(&waiting, &broker, LOCAL, &key(), &route())
            .await
            .is_none()
    );

    let mut five = route();
    five.replicas.push(node("broker-d", 7004));
    five.replicas.push(node("broker-e", 7005));
    let held = Followers(HashMap::from([
        ("broker-b", Some(8)),
        ("broker-c", Some(NOT_A_VOTER)),
        ("broker-d", Some(NOT_A_VOTER)),
        ("broker-e", None),
    ]));
    let probe = probe(&held, &broker, LOCAL, &key(), &five)
        .await
        .expect("a silent member cannot lift a hold");
    assert!(probe.report.caught_up.is_empty() && !probe.reclaimable);
}

/// With five replicas, one refuser leaves three trusted followers, which meet
/// every majority: the furthest is named.
#[tokio::test]
async fn followers_that_meet_every_majority_still_name_the_furthest() {
    let (broker, _dir) = holding(5).await;
    let mut five = route();
    five.replicas.push(node("broker-d", 7004));
    five.replicas.push(node("broker-e", 7005));
    let followers = Followers(HashMap::from([
        ("broker-b", Some(8)),
        ("broker-c", Some(6)),
        ("broker-d", Some(7)),
        ("broker-e", Some(NOT_A_VOTER)),
    ]));

    let probe = probe(&followers, &broker, LOCAL, &key(), &five)
        .await
        .expect("every follower answered");

    assert_eq!(probe.report.caught_up, vec!["broker-b".to_string()]);
}

/// **A pending change's old set must be met too.** Both followers answer,
/// and they meet every majority of the new set, but none belongs to the set
/// being left, which could hold acknowledged records alone: the report is
/// held. Without the pending change the furthest is named.
#[tokio::test]
async fn followers_must_meet_the_set_a_pending_change_leaves() {
    let (broker, _dir) = holding(5).await;
    let followers = Followers(HashMap::from([
        ("broker-b", Some(8)),
        ("broker-c", Some(6)),
    ]));
    let settled = probe(&followers, &broker, LOCAL, &key(), &route())
        .await
        .expect("every follower answered");
    assert_eq!(settled.report.caught_up, vec!["broker-b".to_string()]);

    let mut joint = route();
    joint.pending = Some(Arc::new(felix_router::PendingChange {
        generation: 3,
        leaving: ["broker-a", "broker-d", "broker-e", "broker-f"]
            .map(String::from)
            .to_vec(),
    }));
    let held = probe(&followers, &broker, LOCAL, &key(), &joint)
        .await
        .expect("every follower answered");
    assert!(held.report.given_up && !held.reclaimable);
    assert!(
        held.report.caught_up.is_empty(),
        "{:?}",
        held.report.caught_up
    );
}

fn at(not_behind: bool, furthest: &[&str]) -> LogPosition {
    LogPosition {
        not_behind,
        furthest: furthest.iter().map(|node| node.to_string()).collect(),
    }
}

/// **A cache is never left with neither a promotion nor a reclaim.** A
/// follower ahead on the cache log is named, though every follower trails
/// this broker on the counter log and another is furthest there: the copy
/// is not reclaimable, so naming nobody would leave the shard waiting.
#[test]
fn a_follower_ahead_on_the_cache_log_alone_is_named() {
    let positions = [at(true, &["broker-b"]), at(false, &["broker-c"])];
    assert_eq!(caught_up(&positions), vec!["broker-b".to_string()]);
}

/// A follower furthest on every log is preferred; with none, the furthest on
/// the shard's own log.
#[test]
fn the_follower_furthest_on_every_log_is_named_first() {
    let both = [at(true, &["broker-b", "broker-c"]), at(true, &["broker-c"])];
    assert_eq!(caught_up(&both), vec!["broker-c".to_string()]);
    let split = [at(true, &["broker-b"]), at(true, &["broker-c"])];
    assert_eq!(caught_up(&split), vec!["broker-b".to_string()]);
}

/// **A follower behind this broker on the shard's own log is never named**,
/// however far it is on the counter log: promoting it would lose what this
/// broker kept, so the shard is reclaimed.
#[test]
fn a_follower_behind_on_the_cache_log_is_never_named() {
    let counters_only = [at(false, &["broker-b"]), at(true, &["broker-c"])];
    assert!(caught_up(&counters_only).is_empty());
    let behind_everywhere = [at(false, &["broker-b"]), at(false, &["broker-b"])];
    assert!(caught_up(&behind_everywhere).is_empty());
}

/// **A refused shard's report carries the refusal and nothing measured.** The
/// broker never opened the shard, so there is no tail and no follower
/// position to send; the report replaces the previous one whatever its tail.
#[test]
fn a_refusal_report_carries_the_refusal_and_no_positions() {
    let refused = RefusedGeneration {
        generation: 6,
        promised_to: "broker-d".to_string(),
        ballot: 0,
    };

    let report = refusal_report(&key(), refused.clone());

    assert_eq!(report.generation, 6);
    assert_eq!(report.refused, Some(refused));
    assert!(report.caught_up.is_empty() && report.offsets.is_empty());
    assert!(!report.given_up && !report.drained);
}
