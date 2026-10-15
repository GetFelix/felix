use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering};
use std::time::Duration;

use openraft::error::RPCError;
use openraft::network::{RPCOption, RaftNetwork};
use openraft::raft::AppendEntriesRequest;

use super::*;
use crate::raft::network::HttpNetwork;

/// An application whose join floor the test sets.
pub(in crate::raft) struct FloorApp(pub(in crate::raft) Arc<AtomicU16>);

#[async_trait::async_trait]
impl AppStateMachine for FloorApp {
    async fn apply(&self, _command: &[u8]) -> Vec<u8> {
        Vec::new()
    }
    async fn snapshot(&self) -> Vec<u8> {
        Vec::new()
    }
    async fn restore(&self, _snapshot: &[u8]) {}
    fn version(&self) -> u16 {
        10
    }
    fn join_floor(&self) -> u16 {
        self.0.load(Ordering::SeqCst)
    }
}

pub(in crate::raft) fn hold_at(floor: Arc<AtomicU16>) -> Arc<Hold> {
    hold_with(floor, Arc::default())
}

fn hold_with(floor: Arc<AtomicU16>, versions: Arc<std::sync::Mutex<Versions>>) -> Arc<Hold> {
    Arc::new(Hold::new(Arc::new(FloorApp(floor)), versions))
}

/// A member reporting `version` (`u16::MAX`: not answering) that counts the
/// appends and probes it is sent and answers appends with a conflict while
/// `conflict`.
struct Member {
    version: Arc<AtomicU16>,
    conflict: Arc<AtomicBool>,
    appends: Arc<AtomicUsize>,
    probes: Arc<AtomicUsize>,
    addr: String,
}

async fn member(version: u16) -> Member {
    let member = Member {
        version: Arc::new(AtomicU16::new(version)),
        conflict: Arc::default(),
        appends: Arc::default(),
        probes: Arc::default(),
        addr: String::new(),
    };
    let (version, conflict, appends, probes) = (
        Arc::clone(&member.version),
        Arc::clone(&member.conflict),
        Arc::clone(&member.appends),
        Arc::clone(&member.probes),
    );
    let app = axum::Router::new()
        .route(
            "/internal/raft/append-entries",
            axum::routing::post(move || {
                appends.fetch_add(1, Ordering::SeqCst);
                let reply = if conflict.load(Ordering::SeqCst) {
                    "Conflict"
                } else {
                    "Success"
                };
                async move { axum::Json(serde_json::json!({ "Ok": reply })) }
            }),
        )
        .route(
            "/internal/raft/standing",
            axum::routing::get(move || {
                probes.fetch_add(1, Ordering::SeqCst);
                let version = version.load(Ordering::SeqCst);
                async move {
                    if version == u16::MAX {
                        return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE);
                    }
                    Ok(axum::Json(
                        serde_json::json!({ "last_log_index": 3, "version": version }),
                    ))
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    tokio::spawn(async move { axum::serve(listener, app).await });
    Member { addr, ..member }
}

fn network(addr: &str, hold: Arc<Hold>) -> HttpNetwork {
    HttpNetwork::for_tests(addr, hold)
}

/// An append after `prev`, carrying entries up to `last`.
async fn append_at(
    network: &mut HttpNetwork,
    prev: Option<u64>,
    last: Option<u64>,
) -> Result<(), String> {
    let log_id = |index| openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), index);
    let entries = match (prev, last) {
        (_, None) => Vec::new(),
        (prev, Some(last)) => (prev.map_or(1, |prev| prev + 1)..=last)
            .map(|index| openraft::Entry {
                log_id: log_id(index),
                payload: openraft::EntryPayload::Blank,
            })
            .collect(),
    };
    let request = AppendEntriesRequest {
        vote: openraft::Vote::new(1, 1),
        prev_log_id: prev.map(log_id),
        entries,
        leader_commit: None,
    };
    match network
        .append_entries(request, RPCOption::new(Duration::from_secs(5)))
        .await
    {
        Ok(_) => Ok(()),
        Err(RPCError::Unreachable(err)) => Err(err.to_string()),
        Err(err) => panic!("not an unreachable error: {err:?}"),
    }
}

async fn append(network: &mut HttpNetwork) -> Result<(), String> {
    append_at(network, None, None).await
}

/// What the leader's version watch does with a member's answer.
fn reports(versions: &std::sync::Mutex<Versions>, id: NodeId, version: u16) {
    let mut versions = versions.lock().expect("versions lock");
    let now = versions.epoch();
    versions.record(id, version, now);
}

/// A member that reported a level below the floor gets nothing until it
/// reports the floor.
#[tokio::test]
async fn a_member_below_the_floor_is_held_until_it_reports_it() {
    let member = member(8).await;
    let versions = Arc::<std::sync::Mutex<Versions>>::default();
    let hold = hold_with(Arc::new(AtomicU16::new(10)), Arc::clone(&versions));
    let mut network = network(&member.addr, Arc::clone(&hold));
    reports(&versions, 2, 8);

    let held = append(&mut network).await.expect_err("held");
    assert!(held.contains("supports metadata version 8"), "{held}");
    assert_eq!(
        member.appends.load(Ordering::SeqCst),
        0,
        "a held member was fed"
    );
    assert!(hold.any_held());

    reports(&versions, 2, 10);
    append(&mut network).await.expect("released");
    assert_eq!(member.appends.load(Ordering::SeqCst), 1);
    assert!(!hold.any_held());
}

/// A member whose level is not known, as after a leader change, is fed:
/// the send never asks, and only a reported level below the floor holds.
#[tokio::test]
async fn an_unknown_member_is_fed_without_a_probe() {
    let member = member(u16::MAX).await;
    let mut network = network(&member.addr, hold_at(Arc::new(AtomicU16::new(10))));

    append(&mut network).await.expect("not held");
    append(&mut network).await.expect("not held");
    assert_eq!(member.appends.load(Ordering::SeqCst), 2);
    assert_eq!(
        member.probes.load(Ordering::SeqCst),
        0,
        "the send path probed"
    );
}

/// A lagging member answers conflicts above what it acknowledged while
/// the leader searches for where their logs match. That holds nothing,
/// even when its standing cannot be read.
#[tokio::test]
async fn a_routine_conflict_does_not_hold() {
    let member = member(u16::MAX).await;
    let versions = Arc::<std::sync::Mutex<Versions>>::default();
    let hold = hold_with(Arc::new(AtomicU16::new(10)), Arc::clone(&versions));
    let mut network = network(&member.addr, Arc::clone(&hold));
    reports(&versions, 2, 10);
    append_at(&mut network, None, Some(5)).await.expect("fed");

    member.conflict.store(true, Ordering::SeqCst);
    append_at(&mut network, Some(9), None)
        .await
        .expect("answered");
    append_at(&mut network, Some(7), None)
        .await
        .expect("answered");
    member.conflict.store(false, Ordering::SeqCst);
    append_at(&mut network, Some(6), None)
        .await
        .expect("a routine conflict held the member");
    assert!(!versions.lock().expect("versions lock").is_stale(2));
    assert_eq!(member.probes.load(Ordering::SeqCst), 0);
}

/// A member that conflicts at an entry it acknowledged lost its log, maybe
/// to a restart on an older build. It alone is held, and the watch is
/// woken to ask it, until it reports a level at the floor.
#[tokio::test]
async fn a_member_that_lost_its_log_is_held_until_it_reports() {
    let other = member(10).await;
    let member = member(10).await;
    let versions = Arc::<std::sync::Mutex<Versions>>::default();
    let hold = hold_with(Arc::new(AtomicU16::new(10)), Arc::clone(&versions));
    let mut network = network(&member.addr, Arc::clone(&hold));
    let mut healthy = HttpNetwork::for_tests_as(3, &other.addr, Arc::clone(&hold));
    append_at(&mut network, None, Some(5)).await.expect("fed");

    // Asked before the member lost its log: that answer must not release it.
    let asked_before = versions.lock().expect("versions lock").epoch();
    member.conflict.store(true, Ordering::SeqCst);
    append_at(&mut network, Some(5), None)
        .await
        .expect("answered");
    tokio::time::timeout(Duration::from_secs(1), hold.wake.notified())
        .await
        .expect("the watch was not woken");
    member.conflict.store(false, Ordering::SeqCst);
    let held = append(&mut network).await.expect_err("held");
    assert!(held.contains("lost its log"), "{held}");
    append(&mut healthy)
        .await
        .expect("only the member that lost its log is held");

    versions
        .lock()
        .expect("versions lock")
        .record(2, 10, asked_before);
    append(&mut network)
        .await
        .expect_err("released by an older answer");

    reports(&versions, 2, 8);
    let held = append(&mut network).await.expect_err("held");
    assert!(held.contains("supports metadata version 8"), "{held}");

    reports(&versions, 2, 10);
    append(&mut network).await.expect("released");
}

/// Without a floor nothing is held, whatever a member reported.
#[tokio::test]
async fn nothing_is_held_without_a_floor() {
    let member = member(8).await;
    let versions = Arc::<std::sync::Mutex<Versions>>::default();
    let floor = Arc::new(AtomicU16::new(10));
    let hold = hold_with(Arc::clone(&floor), Arc::clone(&versions));
    let mut network = network(&member.addr, hold);
    reports(&versions, 2, 8);
    append(&mut network).await.expect_err("held");

    floor.store(0, Ordering::SeqCst);
    append(&mut network).await.expect("no floor");
    assert_eq!(member.appends.load(Ordering::SeqCst), 1);
}

/// What the leader's version watch does with one probe of member `id`,
/// sent at `asked_at`.
async fn probe_at(versions: &std::sync::Mutex<Versions>, id: NodeId, addr: &str, asked_at: u64) {
    let answer = crate::raft::version::probe(
        &reqwest::Client::new(),
        &format!("http://{addr}"),
        &crate::raft::version::Asker::default(),
    )
    .await;
    versions
        .lock()
        .expect("versions lock")
        .settle(id, answer, asked_at);
}

async fn probe_now(versions: &std::sync::Mutex<Versions>, id: NodeId, addr: &str) {
    let asked_at = versions.lock().expect("versions lock").epoch();
    probe_at(versions, id, addr, asked_at).await;
}

/// A fresh client's first conflict after `reports(10)`, as when a leader
/// was wiped and restarted on an older build while the others still hold
/// the level it reported before: held, and the watch woken.
async fn first_conflict(
    member: &Member,
    versions: &Arc<std::sync::Mutex<Versions>>,
) -> (Arc<Hold>, HttpNetwork) {
    let hold = hold_with(Arc::new(AtomicU16::new(10)), Arc::clone(versions));
    let mut network = network(&member.addr, Arc::clone(&hold));
    reports(versions, 2, 10);
    member.conflict.store(true, Ordering::SeqCst);
    append_at(&mut network, Some(5), None)
        .await
        .expect("answered");
    tokio::time::timeout(Duration::from_secs(1), hold.wake.notified())
        .await
        .expect("the watch was not woken");
    member.conflict.store(false, Ordering::SeqCst);
    let held = append(&mut network).await.expect_err("held");
    assert!(held.contains("first append"), "{held}");
    (hold, network)
}

/// A first-append conflict from a member on an older build holds it once
/// the probe reports a level below the floor, despite the higher level it
/// reported before.
#[tokio::test]
async fn a_first_conflict_holds_a_member_the_probe_finds_below_the_floor() {
    let member = member(8).await;
    let versions = Arc::<std::sync::Mutex<Versions>>::default();
    let (_hold, mut network) = first_conflict(&member, &versions).await;

    probe_now(&versions, 2, &member.addr).await;
    let held = append(&mut network).await.expect_err("held");
    assert!(held.contains("supports metadata version 8"), "{held}");
    assert_eq!(
        member.appends.load(Ordering::SeqCst),
        1,
        "a member below the floor was fed"
    );
}

/// A member at the floor that conflicts on a first append is only behind:
/// one probe releases it, and later conflicts on the same client do not
/// ask again.
#[tokio::test]
async fn a_first_conflict_from_a_member_at_the_floor_costs_one_probe() {
    let member = member(10).await;
    let versions = Arc::<std::sync::Mutex<Versions>>::default();
    let (hold, mut network) = first_conflict(&member, &versions).await;

    probe_now(&versions, 2, &member.addr).await;
    append(&mut network).await.expect("released");
    member.conflict.store(true, Ordering::SeqCst);
    append_at(&mut network, Some(3), None)
        .await
        .expect("answered");
    member.conflict.store(false, Ordering::SeqCst);
    append(&mut network)
        .await
        .expect("a later conflict held the member");
    assert!(!hold.any_held());
    assert_eq!(member.probes.load(Ordering::SeqCst), 1);
}

/// A probe that gets no answer releases a member held on a first-append
/// conflict: silence never holds. A probe sent before the conflict does
/// not count.
#[tokio::test]
async fn a_failed_probe_releases_a_first_conflict() {
    let member = member(u16::MAX).await;
    let versions = Arc::<std::sync::Mutex<Versions>>::default();
    let asked_before = versions.lock().expect("versions lock").epoch();
    let (hold, mut network) = first_conflict(&member, &versions).await;

    probe_at(&versions, 2, &member.addr, asked_before).await;
    append(&mut network)
        .await
        .expect_err("released by an older probe");

    probe_now(&versions, 2, &member.addr).await;
    append(&mut network).await.expect("released");
    assert!(!hold.any_held());
}

/// The gauge as the Prometheus recorder renders it for `member`.
fn gauge(rendered: &str, member: NodeId) -> Option<String> {
    let prefix = format!("felix_meta_raft_member_below_floor{{member=\"{member}\"}} ");
    rendered
        .lines()
        .find_map(|line| line.strip_prefix(&prefix).map(str::to_string))
}

/// A member removed from the membership while held or marked is
/// forgotten, and its gauge reset; the others are kept.
#[test]
fn a_removed_member_is_forgotten() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let render = recorder.handle();
    let versions = Arc::<std::sync::Mutex<Versions>>::default();
    let hold = hold_with(Arc::new(AtomicU16::new(10)), Arc::clone(&versions));
    reports(&versions, 2, 8);
    reports(&versions, 4, 8);
    metrics::with_local_recorder(&recorder, || {
        hold.check(2).expect_err("held");
        hold.check(4).expect_err("held");
        hold.lost_log(3);
        assert_eq!(gauge(&render.render(), 2).as_deref(), Some("1"));

        hold.retain(&BTreeSet::from([1, 4]));
    });
    assert_eq!(gauge(&render.render(), 2).as_deref(), Some("0"));
    assert_eq!(gauge(&render.render(), 4).as_deref(), Some("1"));
    assert!(!versions.lock().expect("versions lock").is_stale(3));
    assert!(
        hold.any_held(),
        "a member still in the membership was released"
    );
}

/// Stepping down forgets every mark and hold, and resets the gauges.
#[test]
fn stepping_down_forgets_every_hold() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let render = recorder.handle();
    let versions = Arc::<std::sync::Mutex<Versions>>::default();
    let hold = hold_with(Arc::new(AtomicU16::new(10)), Arc::clone(&versions));
    reports(&versions, 2, 8);
    metrics::with_local_recorder(&recorder, || {
        hold.check(2).expect_err("held");
        hold.unconfirmed(3);
        hold.clear();
    });
    assert_eq!(gauge(&render.render(), 2).as_deref(), Some("0"));
    assert!(!versions.lock().expect("versions lock").is_stale(3));
    assert!(!hold.any_held());
}

async fn node(dir: &std::path::Path) -> crate::raft::RaftHandle {
    let mut settings = crate::raft::RaftSettings::new(
        1,
        dir.into(),
        crate::raft::PeerSecurity {
            cluster_id: "test-cluster".to_string(),
            token: Some("0123456789abcdef0123456789abcdef".to_string()),
            tls: None,
        },
    );
    settings.heartbeat_interval = Duration::from_millis(50);
    settings.election_timeout = (Duration::from_millis(150), Duration::from_millis(300));
    crate::raft::RaftHandle::start(settings, Arc::new(FloorApp(Arc::new(AtomicU16::new(10)))))
        .await
        .expect("start raft")
}

async fn until_unmarked(handle: &crate::raft::RaftHandle, id: NodeId) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while handle.versions.lock().expect("versions lock").is_stale(id) || handle.hold.any_held() {
        assert!(
            std::time::Instant::now() < deadline,
            "member {id} is still marked or held"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The leader's version watch drops a member that is not in the membership.
#[tokio::test]
async fn the_leaders_watch_forgets_a_non_member() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handle = node(dir.path()).await;
    handle
        .initialize(BTreeMap::from([(1, "127.0.0.1:0".to_string())]))
        .await
        .expect("initialize");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while handle.status().leader != Some(1) {
        assert!(std::time::Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    reports(&handle.versions, 7, 8);
    handle.hold.check(7).expect_err("held");
    handle.hold.lost_log(7);
    until_unmarked(&handle, 7).await;
    handle.shutdown().await.expect("shutdown");
}

/// A member that does not lead keeps no marks or holds.
#[tokio::test]
async fn a_follower_keeps_no_holds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handle = node(dir.path()).await;
    reports(&handle.versions, 7, 8);
    handle.hold.check(7).expect_err("held");
    handle.hold.lost_log(7);
    until_unmarked(&handle, 7).await;
    handle.shutdown().await.expect("shutdown");
}
