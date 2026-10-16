//! What a probe of a given-up shard's followers reports.
use std::collections::HashMap;
use std::net::SocketAddr;

use bytes::Bytes;
use felix_broker::DurableStorage;
use felix_router::NodeRef;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::FenceOk;

use super::*;
use crate::peer::{PeerError, PeerIdentity};

const LOCAL: &str = "broker-a";

/// Followers answering a fence with where their copies end, or not at all.
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
            Some(log_end) => Ok(InternalMessage::FenceOk(FenceOk {
                correlation_id: 0,
                log_end,
                commit_offset: 0,
                last_generation: 0,
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
/// the first log not behind this broker.
#[test]
fn the_follower_furthest_on_every_log_is_named_first() {
    let both = [at(true, &["broker-b", "broker-c"]), at(true, &["broker-c"])];
    assert_eq!(caught_up(&both), vec!["broker-c".to_string()]);
    let split = [at(true, &["broker-b"]), at(true, &["broker-c"])];
    assert_eq!(caught_up(&split), vec!["broker-b".to_string()]);
    let counters_only = [at(false, &["broker-b"]), at(true, &["broker-c"])];
    assert_eq!(caught_up(&counters_only), vec!["broker-c".to_string()]);
}

/// Nobody is named only when every follower is behind on every log, the one
/// case placement reclaims.
#[test]
fn nobody_is_named_when_every_follower_is_behind_on_every_log() {
    let positions = [at(false, &["broker-b"]), at(false, &["broker-b"])];
    assert!(caught_up(&positions).is_empty());
}
